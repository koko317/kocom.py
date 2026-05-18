#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
 python kocom script

 : forked from script written by vifrost, kyet, 룰루해피, 따분, Susu Daddy, harwin1

 apt-get install mosquitto
 python3 -m pip install pyserial
 python3 -m pip install paho-mqtt
 python3 -m pip install typing_extensions
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

type_t_dic = {'30b':'send', '30d':'ack'}
seq_t_dic = {'c':1, 'd':2, 'e':3, 'f':4}
# device_t_dic = {'01':'wallpad', '0e':'light', '2c':'gas', '36':'thermo', '3b': 'plug', '44':'elevator', '48':'fan'}  # 2023.08 AC, AIR 추가
device_t_dic = {'01': 'wallpad', '0e': 'light', '2c': 'gas', '36': 'thermo', '39': 'ac', '3b': 'plug', '44': 'elevator', '48': 'fan', '98': 'air'}
cmd_t_dic = {'00':'state', '01':'on', '02':'off', '3a':'query'}
room_t_dic = {'00':'livingroom', '01':'room1', '02':'room2', '03':'room3', '04':'kitchen'}

type_h_dic = {v: k for k, v in type_t_dic.items()}
seq_h_dic = {v: k for k, v in seq_t_dic.items()}
device_h_dic = {v: k for k, v in device_t_dic.items()}
cmd_h_dic = {v: k for k, v in cmd_t_dic.items()}
room_h_dic = {'livingroom':'00', 'myhome':'00', 'room1':'01', 'room2':'02', 'room3':'03', 'kitchen':'04'}

# [추가] 에어컨 이전 상태를 기억하기 위한 전역 캐시 변수
global_ac_state = {'state': 'off', 'fan': 'LOW', 'temperature': 24, 'target': 26}


# mqtt functions ----------------------------

def init_mqttc():
    # mqttc = mqtt.Client() # 삭제
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1) # 추가
    mqttc.on_message = mqtt_on_message
    mqttc.on_subscribe = mqtt_on_subscribe
    mqttc.on_connect = mqtt_on_connect
    mqttc.on_disconnect = mqtt_on_disconnect

    if config.get('MQTT','mqtt_allow_anonymous') != 'True':
        logtxt = "[MQTT] connecting (using username and password)"
        mqttc.username_pw_set(username=config.get('MQTT','mqtt_username',fallback=''), password=config.get('MQTT','mqtt_password',fallback=''))
    else:
        logtxt = "[MQTT] connecting (anonymous)"

    mqtt_server = config.get('MQTT','mqtt_server')
    mqtt_port = int(config.get('MQTT','mqtt_port'))
    for retry_cnt in range(1,31):
        try:
            logging.info(logtxt)
            mqttc.connect(mqtt_server, mqtt_port, 60)
            mqttc.loop_start()
            return mqttc
        except:
            logging.error('[MQTT] connection failure. #' + str(retry_cnt))
            time.sleep(10)
    return False

def mqtt_on_subscribe(mqttc, obj, mid, granted_qos):
    logging.info("[MQTT] Subscribed: " + str(mid) + " " + str(granted_qos))

def mqtt_on_log(mqttc, obj, level, string):
    logging.info("[MQTT] on_log : "+string)

def mqtt_on_connect(mqttc, userdata, flags, rc):
    if rc == 0:
        logging.info("[MQTT] Connected - 0: OK")
        mqttc.subscribe('kocom/#', 0)
    else:
        logging.error("[MQTT] Connection error - {}: {}".format(rc, mqtt.connack_string(rc)))

def mqtt_on_disconnect(mqttc, userdata, rc=0):
    logging.error("[MQTT] Disconnected - "+str(rc))


# serial/socket communication class & functions--------------------

class RS485Wrapper:
    def __init__(self, serial_port=None, socket_server=None, socket_port=0):
        if socket_server == None:
            self.type = 'serial'
            self.serial_port = serial_port
        else:
            self.type = 'socket'
            self.socket_server = socket_server
            self.socket_port = socket_port
        self.last_read_time = 0
        self.conn = False

    def connect(self):
        self.close()
        self.last_read_time = 0
        if self.type == 'serial':
            self.conn = self.connect_serial(self.serial_port)
        elif self.type == 'socket':
            self.conn = self.connect_socket(self.socket_server, self.socket_port)
        return self.conn

    def connect_serial(self, SERIAL_PORT):
        if SERIAL_PORT == None:
            os_platfrom = platform.system()
            if os_platfrom == 'Linux':
                SERIAL_PORT = '/dev/ttyUSB0'
            else:
                SERIAL_PORT = 'com3'
        try:
            ser = serial.Serial(SERIAL_PORT, 9600, timeout=1)
            ser.bytesize = 8
            ser.stopbits = 1
            if ser.is_open == False:
                raise Exception('Not ready')
            logging.info('[RS485] Serial connected : {}'.format(ser))
            return ser
        except Exception as e:
            logging.error('[RS485] Serial open failure : {}'.format(e))
            return False

    def connect_socket(self, SOCKET_SERVER, SOCKET_PORT):
        sock = socket.socket()
        sock.settimeout(10)
        try:
            sock.connect((SOCKET_SERVER, SOCKET_PORT))
        except Exception as e:
            logging.error('[RS485] Socket connection failure : {} | server {}, port {}'.format(e, SOCKET_SERVER, SOCKET_PORT))
            return False
        logging.info('[RS485] Socket connected | server {}, port {}'.format(SOCKET_SERVER, SOCKET_PORT))
        sock.settimeout(polling_interval+15)   # set read timeout a little bit more than polling interval
        return sock

    def read(self):
        if self.conn == False:
            return ''
        ret = ''
        if self.type == 'serial':
            for i in range(polling_interval+15):
                try:
                    ret = self.conn.read()
                except AttributeError:
                    raise Exception('exception occured while reading serial')
                except TypeError:
                    raise Exception('exception occured while reading serial')
                if len(ret) != 0:
                    break
        elif self.type == 'socket':
            ret = self.conn.recv(1)

        if len(ret) == 0:
            raise Exception('read byte errror')
        else:
            self.last_read_time = time.time()
        return ret

    def write(self, data):
        if self.conn == False:
            return False
        if self.last_read_time == 0:
            time.sleep(1)
        while time.time() - self.last_read_time < read_write_gap:
            #logging.debug('[**test4**]pending write : time too short after last read')
            time.sleep(max([0, read_write_gap - time.time() + self.last_read_time]))
        if self.type == 'serial':
            return self.conn.write(data)
        elif self.type == 'socket':
            return self.conn.send(data)
        else:
            return False

    def close(self):
        ret = False
        if self.conn != False:
            try:
                ret = self.conn.close()
                self.conn = False
            except:
                pass
        return ret

    def reconnect(self):
        self.close()
        while True:
            logging.info('[RS485] reconnecting to RS485...')
            if self.connect() != False:
                break
            time.sleep(10)



def send(dest, src, cmd, value, log=None, check_ack=True):
    send_lock.acquire()
    ack_data.clear()
    ret = False
    for seq_h in seq_t_dic.keys(): # if there's no ACK received, then repeat sending with next sequence code
        payload = type_h_dic['send'] + seq_h + '00' + dest + src + cmd + value
        send_data = header_h + payload + chksum(payload) + trailer_h
        try:
            if rs485.write(bytearray.fromhex(send_data)) == False:
                raise Exception('Not ready')
        except Exception as ex:
            logging.error("[RS485] Write error.[{}]".format(ex) )
            break
        if log != None:
            logging.info('[SEND|{}] {}'.format(log, send_data))
        if check_ack == False:
            time.sleep(1)
            ret = send_data
            break

        # wait and checking for ACK
        ack_data.append(type_h_dic['ack'] + seq_h + '00' +  src + dest + cmd + value)
        try:
            ack_q.get(True, 1.3+0.2*random.random()) # random wait between 1.3~1.5 seconds for ACK
            if config.get('Log', 'show_recv_hex') == 'True':
                logging.info ('[ACK] OK')
            ret = send_data
            break
        except queue.Empty:
            pass

    if ret == False:
        logging.info('[RS485] send failed. closing RS485. it will try to reconnect to RS485 shortly.')
        rs485.close()
    ack_data.clear()
    send_lock.release()
    return ret


def chksum(data_h):
    sum_buf = sum(bytearray.fromhex(data_h))
    return '{0:02x}'.format((sum_buf)%256)  # return chksum hex value in text format


# hex parsing --------------------------------

def parse(hex_data):
    header_h = hex_data[:4]       # header : aa55
    type_h = hex_data[4:7]        # send/ack : 30b(send) 30d(ack)
    seq_h = hex_data[7:8]         # sequence : c(1st) d(2nd)
    monitor_h = hex_data[8:10]    # monitor : 00(wallpad) 02(KitchenTV)
    dest_h = hex_data[10:14]      # dest addr : 0100(wallpad0) 0e00(light0) 3600(thermo0) 3601(thermo1) 3602(thermo2) 3603(thermo3)
    src_h = hex_data[14:18]       # source addr
    cmd_h = hex_data[18:20]       # command : 3e(query)
    value_h = hex_data[20:36]     # value
    chksum_h = hex_data[36:38]    # checksum
    trailer_h = hex_data[38:42]   # trailer

    data_h = hex_data[4:36]
    payload_h = hex_data[18:36]
    cmd = cmd_t_dic.get(cmd_h)

    ret = { 'header_h':header_h, 'type_h':type_h, 'seq_h':seq_h, 'monitor_h':monitor_h, 'dest_h':dest_h, 'src_h':src_h, 'cmd_h':cmd_h,
            'value_h':value_h, 'chksum_h':chksum_h, 'trailer_h':trailer_h, 'data_h':data_h, 'payload_h':payload_h,
            'type':type_t_dic.get(type_h),
            'seq':seq_t_dic.get(seq_h),
            'dest':device_t_dic.get(dest_h[:2]),
            'dest_subid':str(int(dest_h[2:4], 16)),
            'dest_room':room_t_dic.get(dest_h[2:4]),
            'src':device_t_dic.get(src_h[:2]),
            'src_subid':str(int(src_h[2:4], 16)),
            'src_room':room_t_dic.get(src_h[2:4]),
            'cmd':cmd if cmd!=None else cmd_h,
            'value':value_h,
            'time':time.time(),
            'flag':None}
    return ret


def thermo_parse(value):
    ret = { 'heat_mode': 'heat' if value[:2] == '11' else 'off',
            'away': 'true' if value[2:4] == '01' else 'false',
            'set_temp': int(value[4:6], 16) if value[:2] == '11' else int(config.get('User', 'init_temp')),
            'cur_temp': int(value[8:10], 16)}
    return ret


def light_parse(value):
    ret = {}
    for i in range(1, int(config.get('User', 'light_count'))+1):
        ret['light_'+str(i)] = 'off' if value[i*2-2:i*2] == '00' else 'on'
    return ret


def fan_parse(value):
    preset_dic = {'40':'Low', '80':'Medium', 'c0':'High'}
#   state = 'off' if value[:2] == '10' else 'on'
    state = 'off' if value[:2] == '00' else 'on'
    preset = 'Off' if state == 'off' else preset_dic.get(value[4:6])
    logtxt='[MQTT Parse | Fan] value[{}], state[{}]'.format(value, state)    # 20221108 주석기능 추가
    if logtxt != "" and config.get('Log', 'show_recv_hex') == 'True':
        logging.info(logtxt)
    return { 'state': state, 'preset': preset}


# [수정] 에어컨 데이터 깨짐 및 값 튀는 현상 방지 로직 도입
def ac_parse(value):
    global global_ac_state
    
    mode_dic = {'00': 'cool', '01': 'fan_only', '02': 'dry', '03': 'auto'}
    spd_dic = {'01': 'LOW', '02': 'MEDIUM', '03': 'HIGH'}
    
    # 1. 패킷 종류 판별 (앞 2자리가 10 또는 00인 전원/모드/풍량 패킷인지 확인)
    if value[:2] in ['00', '10']:
        state = mode_dic.get(value[2:4]) if value[:2] == '10' else 'off'
        fan = spd_dic.get(value[4:6], global_ac_state['fan'])
        
        # 상태 업데이트
        global_ac_state['state'] = state
        global_ac_state['fan'] = fan
        
    else:
        # 2. 전원 패킷이 아닐 경우 (온도값만 들어오는 상태 패킷일 때)
        try:
            temperature = int(value[8:10], 16)
            target = int(value[10:12], 16)
            
            # 유효한 온도 범위 내에 있을 때만 캐시를 업데이트하여 값 튀는 현상 방지
            if 10 <= temperature <= 40:
                global_ac_state['temperature'] = temperature
            if 15 <= target <= 35:
                global_ac_state['target'] = target
        except ValueError:
            pass

    logtxt = '[MQTT Parse | Ac Cache] value[{}], current_state[{}]'.format(value, global_ac_state)
    if logtxt != '' and config.get('Log', 'show_recv_hex') == 'True':
        logging.info(logtxt)
        
    return global_ac_state


# query device --------------------------

def query(device_h, publish=False, enforce=False):
    # find from the cache first
    for c in cache_data:
        if enforce: break
        if time.time() - c['time'] > polling_interval:  # if there's no data within polling interval, then exit cache search
            break
        if c['type'] == 'ack' and c['src'] == 'wallpad' and c['dest_h'] == device_h and c['cmd'] != 'query':
            if (config.get('Log', 'show_query_hex') == 'True'):
                logging.info('[cache|{}{}] query cache {}'.format(c['dest'], c['dest_subid'], c['data_h']))
            return c  # return the value in the cache

    # if there's no cache data within polling inteval, then send query packet
    if (config.get('Log', 'show_query_hex') == 'True'):
        log = 'query ' + device_t_dic.get(device_h[:2]) + str(int(device_h[2:4],16))
    else:
        log = None
    return send_wait_response(dest=device_h, cmd=cmd_h_dic['query'], log=log, publish=publish)


def send_wait_response(dest, src=device_h_dic['wallpad']+'00', cmd=cmd_h_dic['state'], value='0'*16, log=None, check_ack=True, publish=True):
    #logging.debug('[**test1**]waiting for send_wait_response :'+dest)
    wait_target.put(dest)
    #logging.debug('[**test2**]entered send_wait_response :'+dest)
    ret = { 'value':'0'*16, 'flag':False }

    if send(dest, src, cmd, value, log, check_ack) != False:
        try:
            ret = wait_q.get(True, 2)
            if publish == True:
                publish_status(ret)
        except queue.Empty:
            pass
    wait_target.get()
    #logging.debug('[**test3**]exiting send_wait_response :'+dest)
    return ret


#===== elevator call via TCP/IP =====

def call_elevator_tcpip():
    import socket
    sock = socket.socket()
    sock.settimeout(10)

    APT_SERVER = config.get('Elevator', 'tcpip_apt_server')
    APT_PORT = int(config.get('Elevator', 'tcpip_apt_port'))

    try:
        sock.connect((APT_SERVER, APT_PORT))
    except Exception as e:
        logging.error('Apartment server socket connection failure : {} | server {}, port {}'.format(e, APT_SERVER, APT_PORT))
        return False
    logging.info('Apartment server socket connected | server {}, port {}'.format(APT_SERVER, APT_PORT))

    try:
        sock.send(bytearray.fromhex(config.get('Elevator', 'tcpip_packet1')))
        rcv = sock.recv(512)
        logging.info('recv from apt server: '+''.join("%02x" % i for i in rcv) )
        time.sleep(0.1)
        sock.send(bytearray.fromhex(config.get('Elevator', 'tcpip_packet2')))
        rcv = sock.recv(512)
        logging.info('recv from apt server: '+''.join("%02x" % i for i in rcv) )
        sock.send(bytearray.fromhex(config.get('Elevator', 'tcpip_packet3')))
        for itr in range(100):
            rcv = sock.recv(512)
            if len(rcv) == 0:
                logging.info('apt server connection closed by peer')
                sock.close()
                return True
            rcv_hex = ''.join("%02x" % i for i in rcv)
            logging.info('recv from apt server: '+rcv_hex )
            if rcv_hex == config.get('Elevator', 'tcpip_packet4'):
                logging.info('elevator arrived. sending last heartbeat' )
                break
        sock.send(bytearray.fromhex(config.get('Elevator', 'tcpip_packet2')))
        rcv = sock.recv(512)
        logging.info('recv from apt server: '+''.join("%02x" % i for i in rcv) )
        sock.close()
    except Exception as e:
        logging.error('Apartment server socket communication failure : {}'.format(e))
        return False

    return True


#===== parse MQTT --> send hex packet =====

def mqtt_on_message(mqttc, obj, msg):
    command = msg.payload.decode('ascii')
    topic_d = msg.topic.split('/')

    # do not process other than command topic
    if topic_d[-1] != 'command':
        return

    logging.info("[MQTT RECV] " + msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

    # thermo heat/off : kocom/room/thermo/3/heat_mode/command
    if 'thermo' in topic_d and 'heat_mode' in topic_d:
#       heatmode_dic = {'heat': '11', 'off': '01'}
        heatmode_dic = {'heat': '11', 'off': '00'}

        dev_id = device_h_dic['thermo']+'{0:02x}'.format(int(topic_d[3]))
        q = query(dev_id)
        #settemp_hex = q['value'][4:6] if q['flag']!=False else '14'
        settemp_hex = '{0:02x}'.format(int(config.get('User', 'thermo_init_temp'))) if q['flag']!=False else '14'
        value = heatmode_dic.get(command) + '00' + settemp_hex + '0000000000'
        send_wait_response(dest=dev_id, value=value, log='thermo heatmode')

    # thermo set temp : kocom/room/thermo/3/set_temp/command
    elif 'thermo' in topic_d and 'set_temp' in topic_d:
        dev_id = device_h_dic['thermo']+'{0:02x}'.format(int(topic_d[3]))
        settemp_hex = '{0:02x}'.format(int(float(command)))

        value = '1100' + settemp_hex + '0000000000'
        send_wait_response(dest=dev_id, value=value, log='thermo settemp')

 # 2023.08 AC 추가
    elif 'ac' in topic_d and 'ac_mode' in topic_d:
        is_on = '10' if command != 'off' else '00'
        acmode_dic = {'off': '00', 'cool': '00','fan_only': '01', 'dry': '02', 'auto': '03'}
        dev_id = device_h_dic['ac']+'{0:02x}'.format(int(topic_d[3]))
        #q = query(dev_id)
        #settemp_hex = '{0:02x}'.format(int(config.get('User', 'ac_init_temp'))) if q['flag'] != False else '12'
        
        value = is_on + acmode_dic.get(command, config.get('User', 'ac_init_mode')) + '000000000000'
        send_wait_response(dest=dev_id, value=value, log='ac mode')

    elif 'ac' in topic_d and 'fan_mode' in topic_d:
        fan_dic = {'LOW': '01', 'MEDIUM': '02', 'HIGH': '03'}
        dev_id = device_h_dic['ac']+'{0:02x}'.format(int(topic_d[3]))
        #q = query(dev_id)
        #settemp_hex = '{0:02x}'.format(int(config.get('User', 'ac_init_temp'))) if q['flag'] != False else '12'
        
        value = '1010' + fan_dic.get(command, config.get('User', 'ac_init_fan_mode')) + '0000000000'
        send_wait_response(dest=dev_id, value=value, log='ac mode')
        
    # ac set temp : kocom/room/ac/3/set_temp/command
    elif 'ac' in topic_d and 'set_temp' in topic_d:
        dev_id = device_h_dic['ac']+'{0:02x}'.format(int(topic_d[3]))
