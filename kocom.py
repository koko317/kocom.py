#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
 python kocom script

 : forked from script written by vifrost, kyet, 룰루해피, 따분, Susu Daddy, harwin1
'''
import os
import time
import platform
import threading
import queue
import random
import json
import paho.mqtt.client as mqtt
import logging
import configparser


# define -------------------------------
SW_VERSION = '2024.08.06'
CONFIG_FILE = 'kocom.conf'
BUF_SIZE = 100

read_write_gap = 0.03  # minimal time interval between last read to write
polling_interval = 300  # polling interval

header_h = 'aa55'
trailer_h = '0d0d'
packet_size = 21  # total 21bytes
chksum_position = 18  # 18th byte

type_t_dic = {'30b':'send', '30d':'ack'}\nseq_t_dic = {'c':1, 'd':2, 'e':3, 'f':4}
device_t_dic = {'01':'wallpad', '0e':'light', '2c':'gas', '36':'thermo', '3b': 'plug', '44':'elevator', '48':'fan', '3c':'ac'}

device_h_dic = {v:k for k, v in device_t_dic.items()}

# 에어컨 상태 누적 관리를 위한 캐시 전역 변수
ac_state_cache = {}


# --------------------------------------
# RS485 wrapper class
# --------------------------------------
class RS485Wrapper:
    def __init__(self, serial_port=None, socket_server=None, socket_port=None):
        if serial_port != None:
            self.type = 'serial'
            self.serial_port = serial_port
        elif socket_server != None and socket_port != None:
            self.type = 'socket'
            self.socket_server = socket_server
            self.socket_port = socket_port
        else:
            self.type = 'none'
            
        self.last_read_time = time.time()
        self.last_write_time = time.time()

    def connect(self):
        if self.type == 'serial':
            try:
                import serial
                self.conn = serial.Serial(self.serial_port, 9600, timeout=0.05)
                return True
            except Exception as e:
                logging.error('[RS485] serial connection error: {}'.format(e))
                return False
        elif self.type == 'socket':
            try:
                import socket
                self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.conn.connect((self.socket_server, self.socket_port))
                self.conn.settimeout(0.05)
                return True
            except Exception as e:
                logging.error('[RS485] socket connection error: {}'.format(e))
                return False
        else:
            logging.error('[RS485] invalid connection type')
            return False

    def read(self):
        data = b''
        if self.type == 'serial':
            try:
                if self.conn.in_waiting > 0:
                    data = self.conn.read(self.conn.in_waiting)
                    self.last_read_time = time.time()
            except Exception as e:
                logging.error('[RS485] serial read error: {}'.format(e))
        elif self.type == 'socket':
            try:
                data = self.conn.recv(BUF_SIZE)
                self.last_read_time = time.time()
            except socket.timeout:
                pass
            except Exception as e:
                logging.error('[RS485] socket read error: {}'.format(e))
        return data

    def write(self, data):
        elapsed_time = time.time() - self.last_read_time
        if elapsed_time < read_write_gap:
            time.sleep(read_write_gap - elapsed_time)
        
        if self.type == 'serial':
            try:
                self.conn.write(data)
                self.last_write_time = time.time()
                return True
            except Exception as e:
                logging.error('[RS485] serial write error: {}'.format(e))
                return False
        elif self.type == 'socket':
            try:
                self.conn.sendall(data)
                self.last_write_time = time.time()
                return True
            except Exception as e:
                logging.error('[RS485] socket write error: {}'.format(e))
                return False
        return False


# --------------------------------------
# 에어컨 전용 파싱 함수 (캐시 적용 및 튀는 값 방지)
# --------------------------------------
def ac_parse(value, device_id):
    global ac_state_cache
    
    try:
        dev_key = str(int(device_id, 16))
    except:
        dev_key = str(device_id)
        
    if dev_key not in ac_state_cache:
        ac_state_cache[dev_key] = {'state': 'off', 'fan': 'LOW', 'temperature': 24, 'target': 26}
        
    mode_dic = {'00': 'cool', '01': 'fan_only', '02': 'dry', '03': 'auto'}
    spd_dic = {'01': 'LOW', '02': 'MEDIUM', '03': 'HIGH'}
    
    packet_type = value[:2] 
    
    if packet_type == '10':
        state_hex = value[2:4]
        if state_hex != '00':
            ac_state_cache[dev_key]['state'] = mode_dic.get(state_hex, 'cool')
        else:
            ac_state_cache[dev_key]['state'] = 'off'
            
        fan_hex = value[4:6]
        if fan_hex in spd_dic:
            ac_state_cache[dev_key]['fan'] = spd_dic[fan_hex]
            
    else:
        try:
            cur_temp = int(value[8:10], 16)
            tar_temp = int(value[10:12], 16)
            
            if 10 <= cur_temp <= 40:
                ac_state_cache[dev_key]['temperature'] = cur_temp
            if 10 <= tar_temp <= 35:
                ac_state_cache[dev_key]['target'] = tar_temp
        except ValueError:
            pass 

    if config.get('Log', 'show_recv_hex') == 'True':
        logging.info('[AC Cache Update] ID: {} -> {}'.format(dev_key, ac_state_cache[dev_key]))
        
    return ac_state_cache[dev_key]


# --------------------------------------
# 패킷 변환 및 유효성 검사 함수들
# --------------------------------------
def hex_to_packet(hex_string):
    p = {}
    try:
        p['header'] = hex_string[:4]
        p['type'] = type_t_dic.get(hex_string[4:7], hex_string[4:7])
        p['seq'] = seq_t_dic.get(hex_string[7:8], hex_string[7:8])
        p['dest_h'] = hex_string[8:12]
        p['dest'] = device_t_dic.get(hex_string[8:10], hex_string[8:10])
        p['dest_subid'] = hex_string[10:12]
        p['src_h'] = hex_string[12:16]
        p['src'] = device_t_dic.get(hex_string[12:14], hex_string[12:14])
        p['src_subid'] = hex_string[14:16]
        p['cmd'] = 'state' if hex_string[16:18] == '00' else hex_string[16:18]
        p['value'] = hex_string[18:34]
        p['chksum'] = hex_string[34:36]
        p['trailer'] = hex_string[36:42]
    except Exception as e:
        logging.error('[HEX TO PACKET] mapping error: {}'.format(e))
    return p

def chksum_calc(hex_string):
    try:
        sum_buf = 0
        for i in range(2, chksum_position):
            sum_buf += int(hex_string[i*2:i*2+2], 16)
        return '{:02x}'.format((sum_buf + 1) % 256)
    except Exception as e:
        logging.error('[CHKSUM CALC] error: {}'.format(e))
        return '00'

def chksum_validate(hex_string):
    if len(hex_string) != packet_size * 2:
        return False
    return hex_string[chksum_position*2:chksum_position*2+2] == chksum_calc(hex_string)


# --------------------------------------
# 시리얼 통신 쓰레드 및 파서
# --------------------------------------
def read_thread():
    hex_string = ''
    while True:
        data = rs485.read()
        if len(data) > 0:
            hex_string += data.hex()
            
            while len(hex_string) >= packet_size * 2:
                idx = hex_string.find(header_h)
                if idx == -1:
                    hex_string = ''
                    break
                elif idx > 0:
                    hex_string = hex_string[idx:]
                    continue
                    
                if len(hex_string) < packet_size * 2:
                    break
                    
                target_packet = hex_string[:packet_size*2]
                hex_string = hex_string[packet_size*2:]
                
                if target_packet.endswith(trailer_h):
                    if chksum_validate(target_packet):
                        msg_q.put(target_packet)
                    else:
                        logging.warning('[RECV] checksum error: {}'.format(target_packet))
                else:
                    logging.warning('[RECV] invalid trailer: {}'.format(target_packet))
        time.sleep(0.01)

def packet_processor():
    while True:
        try:
            packet_hex = msg_q.get(block=True, timeout=1)
        except queue.Empty:
            continue
            
        p = hex_to_packet(packet_hex)
        
        if config.get('Log', 'show_recv_hex') == 'True':
            logging.info('[RECV] type[{}] seq[{}] src[{}:{}] dest[{}:{}] cmd[{}] val[{}]'.format(p['type'], p['seq'], p['src'], p['src_subid'], p['dest'], p['dest_subid'], p['cmd'], p['value']))
            
        if p['type'] == 'ack':
            ack_data.append(packet_hex)
            if not ack_q.full():
                ack_q.put(packet_hex)
            msg_q.task_done()
            continue
            
        # MQTT 상태 Publish 로직
        if p['src'] == 'light' and p['cmd'] == 'state':
            for i in range(4):
                state = 'ON' if p['value'][i*2:i*2+2] == 'ff' else 'OFF'
                mqttc.publish('kocom/room/light/' + p['src_subid'] + '/' + str(i+1) + '/state', state, retain=True)
                
        elif p['src'] == 'gas' and p['cmd'] == 'state':
            state = 'ON' if p['value'][:2] == '01' else 'OFF'
            mqttc.publish('kocom/room/gas/' + p['src_subid'] + '/state', state, retain=True)
            
        elif p['src'] == 'thermo' and p['cmd'] == 'state':
            state = 'ON' if p['value'][:2] == '11' else 'OFF'
            cur_temp = int(p['value'][2:4], 16)
            tar_temp = int(p['value'][4:6], 16)
            data = {'state': state, 'temperature': cur_temp, 'target': tar_temp}
            mqttc.publish('kocom/room/thermo/' + p['src_subid'] + '/state', json.dumps(data), retain=True)
            
        elif p['src'] == 'elevator' and p['cmd'] == 'state':
            state = 'ON' if p['value'][:2] == '01' else 'OFF'
            floor = p['value'][2:4]
            data = {'state': state, 'floor': floor}
            mqttc.publish('kocom/room/elevator/' + p['src_subid'] + '/state', json.dumps(data), retain=True)
            
        # [수정] 에어컨 상태 파싱 인자 불일치 해결 및 안정성 확보
        elif p['src'] == 'ac' and p['cmd'] == 'state':
            state = ac_parse(p['value'], p['src_subid'])
            mqttc.publish('kocom/room/ac/' + p['src_subid'] + '/state', json.dumps(state), retain=True)
            
        msg_q.task_done()


# --------------------------------------
# 데이터 전송 관리 함수
# --------------------------------------
def send_packet(packet_hex):
    with send_lock:
        if config.get('Log', 'show_query_hex') == 'True':
            p = hex_to_packet(packet_hex)
            logging.info('[SEND] type[{}] seq[{}] src[{}:{}] dest[{}:{}] cmd[{}] val[{}]'.format(p['type'], p['seq'], p['src'], p['src_subid'], p['dest'], p['dest_subid'], p['cmd'], p['value']))
        return rs485.write(bytes.fromhex(packet_hex))

def send_wait_response(dest, value, log='', cmd='00', src='0100', type_t='30b', seq_t='c'):
    packet_hex = header_h + type_t + seq_t + dest + src + cmd + value
    packet_hex = packet_hex + chksum_calc(packet_hex) + trailer_h
    
    try:
        wait_q.put(packet_hex, block=True, timeout=2)
        wait_target.put(dest, block=True, timeout=2)
    except queue.Full:
        logging.warning('[QUEUE] send queue is full. clip packet')


def tx_thread():
    while True:
        try:
            packet_hex = wait_q.get(block=True, timeout=1)
            dest = wait_target.get()
        except queue.Empty:
            continue
            
        p = hex_to_packet(packet_hex)
        retry_cnt = 0
        ack_success = False
        
        while retry_cnt < 3:
            while not ack_q.empty():
                ack_q.get()
            
            send_packet(packet_hex)
            
            try:
                ack_packet = ack_q.get(block=True, timeout=0.15)
                ap = hex_to_packet(ack_packet)
                if ap['src_h'].lower() == dest.lower():
                    ack_success = True
                    break
            except queue.Empty:
                retry_cnt += 1
                time.sleep(0.05)
                
        if not ack_success:
            logging.warning('[TX] ACK fail for destination [{}], retried 3 times'.format(dest))
            
        wait_q.task_done()
        wait_target.task_done()
        time.sleep(read_write_gap)


# --------------------------------------
# MQTT & 홈어시스턴트 자동 등록(Discovery)
# --------------------------------------
def discovery():
    enabled_devices = [x.strip() for x in config.get('User', 'enabled').split(',')]
    
    for dev in enabled_devices:
        if 'light' in dev:
            room_id = dev.replace('light_room', '').replace('light_livingroom', '0')
            for i in range(1, 5):
                topic = 'homeassistant/light/kocom_light_{}_{}/config'.format(room_id, i)
                payload = {
                    "name": "Kocom Light {} {}".format(room_id, i),
                    "cmd_t": "kocom/room/light/{}/{}/command".format(room_id, i),
                    "stat_t": "kocom/room/light/{}/{}/state".format(room_id, i),
                    "uniq_id": "kocom_light_{}_{}".format(room_id, i)
                }
                mqttc.publish(topic, json.dumps(payload), retain=True)
                
        elif 'gas' in dev:
            topic = 'homeassistant/switch/kocom_gas/config'
            payload = {
                "name": "Kocom Gas Valve",
                "cmd_t": "kocom/room/gas/0/command",
                "stat_t": "kocom/room/gas/0/state",
                "uniq_id": "kocom_gas_valve",
                "icon": "mdi:gas-cylinder"
            }
            mqttc.publish(topic, json.dumps(payload), retain=True)
            
        elif 'thermo' in dev:
            room_id = dev.replace('thermo_room', '').replace('thermo_livingroom', '0')
            topic = 'homeassistant/climate/kocom_thermo_{}/config'.format(room_id)
            payload = {
                "name": "Kocom Thermostat {}".format(room_id),
                "mode_cmd_t": "kocom/room/thermo/{}/mode/command".format(room_id),
                "mode_stat_t": "kocom/room/thermo/{}/state".format(room_id),
                "mode_stat_tpl": "{{ value_json.state }}",
                "temp_cmd_t": "kocom/room/thermo/{}/temp/command".format(room_id),
                "temp_stat_t": "kocom/room/thermo/{}/state".format(room_id),
                "temp_stat_tpl": "{{ value_json.target }}",
                "curr_temp_t": "kocom/room/thermo/{}/state".format(room_id),
                "curr_temp_tpl": "{{ value_json.temperature }}",
                "modes": ["off", "heat"],
                "min_temp": 5, "max_temp": 40, "temp_step": 1,
                "uniq_id": "kocom_thermo_{}".format(room_id)
            }
            mqttc.publish(topic, json.dumps(payload), retain=True)

        elif 'ac' in dev:
            room_id = dev.replace('ac_room', '').replace('ac_livingroom', '0')
            topic = 'homeassistant/climate/kocom_ac_{}/config'.format(room_id)
            payload = {
                "name": "Kocom AC {}".format(room_id),
                "mode_cmd_t": "kocom/room/ac/{}/ac_mode/command".format(room_id),
                "mode_stat_t": "kocom/room/ac/{}/state".format(room_id),
                "mode_stat_tpl": "{{ value_json.state }}",
                "fan_mode_cmd_t": "kocom/room/ac/{}/fan_mode/command".format(room_id),
                "fan_mode_stat_t": "kocom/room/ac/{}/state".format(room_id),
                "fan_mode_stat_tpl": "{{ value_json.fan }}",
                "temp_cmd_t": "kocom/room/ac/{}/set_temp/command".format(room_id),
                "temp_stat_t": "kocom/room/ac/{}/state".format(room_id),
                "temp_stat_tpl": "{{ value_json.target }}",
                "curr_temp_t": "kocom/room/ac/{}/state".format(room_id),
                "curr_temp_tpl": "{{ value_json.temperature }}",
                "modes": ["off", "cool", "fan_only", "dry", "auto"],
                "fan_modes": ["LOW", "MEDIUM", "HIGH"],
                "min_temp": 18, "max_temp": 30, "temp_step": 1,
                "uniq_id": "kocom_ac_{}".format(room_id)
            }
            mqttc.publish(topic, json.dumps(payload), retain=True)


def mqtt_on_message(client, userdata, msg):
    command = msg.payload.decode('utf-8')
    topic_d = msg.topic.split('/')
    
    if config.get('Log', 'show_mqtt_publish') == 'True':
        logging.info('[MQTT RECV] topic: {}, command: {}'.format(msg.topic, command))
        
    if 'light' in topic_d:
        dev_id = device_h_dic['light'] + '{:02x}'.format(int(topic_d[3]))
        switch_idx = int(topic_d[4]) - 1
        value = '0000000000000000'
        # 통상 kocom 등 조명 제어 패킷 생성 로직 호출 가능 (필요시 구현)
        
    elif 'gas' in topic_d:
        dev_id = device_h_dic['gas'] + '00'
        if command == 'OFF':
            send_wait_response(dest=dev_id, value='0000000000000000', log='gas off')
            
    elif 'thermo' in topic_d and 'mode' in topic_d:
        dev_id = device_h_dic['thermo'] + '{:02x}'.format(int(topic_d[3]))
        val = '11' if command == 'heat' else '01'
        send_wait_response(dest=dev_id, value=val + '00000000000011', log='thermo mode')
        
    elif 'thermo' in topic_d and 'temp' in topic_d:
        dev_id = device_h_dic['thermo'] + '{:02x}'.format(int(topic_d[3]))
        temp_hex = '{:02x}'.format(int(float(command)))
        send_wait_response(dest=dev_id, value='1100' + temp_hex + '0000000011', log='thermo temp')

    # [수정] 캐시 연동형 에어컨 MQTT 제어부 문법 오류 전면 수정
    elif 'ac' in topic_d and 'ac_mode' in topic_d:
        dev_id = device_h_dic['ac'] + '{:02x}'.format(int(topic_d[3]))
        dev_key = str(int(topic_d[3]))
        current_cache = ac_state_cache.get(dev_key, {'state':'off', 'fan':'LOW', 'target':25})
        
        is_on = '10' if command != 'off' else '00'
        acmode_dic = {'off': '00', 'cool': '00', 'fan_only': '01', 'dry': '02', 'auto': '03'}
        
        settemp_hex = '{:02x}'.format(int(current_cache['target']))
        value = is_on + acmode_dic.get(command, '00') + '000000' + settemp_hex + '0000'
        send_wait_response(dest=dev_id, value=value, log='ac mode')

    elif 'ac' in topic_d and 'fan_mode' in topic_d:
        dev_id = device_h_dic['ac'] + '{:02x}'.format(int(topic_d[3]))
        dev_key = str(int(topic_d[3]))
        current_cache = ac_state_cache.get(dev_key, {'state':'cool', 'fan':'LOW', 'target':25})
        
        fan_dic = {'LOW': '01', 'MEDIUM': '02', 'HIGH': '03'}
        is_on = '10' if current_cache['state'] != 'off' else '00'
        
        settemp_hex = '{:02x}'.format(int(current_cache['target']))
        value = is_on + '00' + fan_dic.get(command, '01') + '000000' + settemp_hex + '0000'
        send_wait_response(dest=dev_id, value=value, log='ac fan_mode')
        
    elif 'ac' in topic_d and 'set_temp' in topic_d:
        dev_id = device_h_dic['ac'] + '{:02x}'.format(int(topic_d[3]))
        dev_key = str(int(topic_d[3]))
        current_cache = ac_state_cache.get(dev_key, {'state':'cool', 'fan':'LOW', 'target':25})
        
        is_on = '10' if current_cache['state'] != 'off' else '00'
        settemp_hex = '{:02x}'.format(int(float(command)))

        value = is_on + '00000000' + settemp_hex + '0000'
        send_wait_response(dest=dev_id, value=value, log='ac settemp')


def init_mqttc():
    try:
        client = mqtt.Client()
        client.on_message = mqtt_on_message
        
        if config.get('MQTT', 'mqtt_allow_anonymous') == 'False':
            client.username_pw_set(config.get('MQTT', 'mqtt_username'), config.get('MQTT', 'mqtt_password'))
            
        client.connect(config.get('MQTT', 'mqtt_server'), int(config.get('MQTT', 'mqtt_port')), 60)
        client.loop_start()
        
        client.subscribe('kocom/room/+/+/command')
        client.subscribe('kocom/room/+/+/+/command')
        return client
    except Exception as e:
        logging.error('[MQTT] init error: {}'.format(e))
        return False


# --------------------------------------
# Main 루프 구동
# --------------------------------------
if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s[%(asctime)s]:%(message)s ', level=logging.DEBUG)

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    if config.get('RS485', 'type') == 'serial':
        import serial
        rs485 = RS485Wrapper(serial_port = config.get('RS485', 'serial_port', fallback=None))
    elif config.get('RS485', 'type') == 'socket':
        import socket
        rs485 = RS485Wrapper(socket_server = config.get('RS485', 'socket_server'), socket_port = int(config.get('RS485', 'socket_port')))
    else:
        logging.error('[CONFIG] invalid type value in [RS485]. exit')
        exit(1)
        
    if rs485.connect() == False:
        logging.error('[RS485] connection error. exit')
        exit(1)

    mqttc = init_mqttc()
    if mqttc == False:
        exit(1)

    msg_q = queue.Queue(BUF_SIZE)
    ack_q = queue.Queue(1)
    ack_data = []
    wait_q = queue.Queue(1)
    wait_target = queue.Queue(1)
    send_lock = threading.Lock()

    # 스레드 기동
    t1 = threading.Thread(target=read_thread)
    t1.daemon = True
    t1.start()

    t2 = threading.Thread(target=packet_processor)
    t2.daemon = True
    t2.start()

    t3 = threading.Thread(target=tx_thread)
    t3.daemon = True
    t3.start()

    # Home Assistant 기기 등록 및 루프 유지
    discovery()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Exit script")
