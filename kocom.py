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

# ── 삼성 에어컨 이중 패킷 대응 ──────────────────────────────────────────────────
# Samsung AC는 하나의 명령/쿼리에 대해 상태 패킷을 2번 전송합니다.
# 첫 번째 패킷: 부분 정보 (온도=0 등 불완전할 수 있음)
# 두 번째 패킷: 완전한 최종 상태
# 아래 값(초)을 0으로 설정하면 기존 단일 패킷 방식으로 동작합니다.
AC_DUAL_PACKET_TIMEOUT = 0.8  # 두 번째 AC 패킷을 기다리는 최대 시간(초)

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


# ── ACK 매칭 헬퍼 ────────────────────────────────────────────────────────────
# [수정] 삼성 에어컨 대응: 접두사(prefix) 매칭 지원
# 기존 기기: ack_data에 전체 32자 패턴 저장 → 완전 일치 검사
# 삼성 AC:  ack_data에 14자 접두사만 저장 → 시작 부분 일치 검사
# 이렇게 하면 AC가 ACK에 현재 상태값(value)을 실어 보내도 정상 인식됩니다.
def ack_matches(data_h, ack_patterns):
    """ACK 패턴 매칭: 완전 일치 또는 접두사 일치를 모두 지원"""
    for pattern in ack_patterns:
        if data_h[:len(pattern)] == pattern:
            return True
    return False


def send(dest, src, cmd, value, log=None, check_ack=True, ack_value_check=True):
    """
    RS485로 패킷을 전송합니다.

    ack_value_check=True  (기본값): ACK 검증 시 cmd+value까지 비교 (기존 기기용)
    ack_value_check=False          : ACK 검증 시 type+seq+src+dest만 비교 (삼성 AC용)
                                     삼성 AC는 ACK 패킷에 현재 전체 상태를 실어 보내므로
                                     보낸 value와 ACK의 value가 달라도 정상으로 처리합니다.
    """
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
        if ack_value_check:
            # 기존 방식: type + seq + monitor + src(원래dest) + dest(원래src) + cmd + value 완전 일치
            ack_data.append(type_h_dic['ack'] + seq_h + '00' + src + dest + cmd + value)
        else:
            # 삼성 AC 방식: type + seq + monitor + src(원래dest) + dest(원래src) 만 비교
            # value 필드는 무시 → AC가 현재 상태값을 ACK에 실어 보내도 정상 인식
            ack_data.append(type_h_dic['ack'] + seq_h + '00' + src + dest)
            logging.debug('[ACK] Samsung AC mode: partial ACK matching (ignoring value field)')

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


# 2023.08 AC 추가 / 삼성 AC 대응 수정
def ac_parse(value):
    """
    AC 상태 패킷 파싱 (value 16자 = 8바이트)
      [0:2]  = 전원 (10=켜짐, 00=꺼짐)
      [2:4]  = 모드 (00=cool, 01=fan_only, 02=dry, 03=auto)
      [4:6]  = 팬속도 (01=LOW, 02=MEDIUM, 03=HIGH)
      [6:8]  = 예약/미사용
      [8:10] = 현재 실내 온도 (hex)
      [10:12]= 설정 온도 (hex)
      [12:16]= 예약/미사용
    """
    mode_dic = {'00': 'cool', '01': 'fan_only', '02': 'dry', '03': 'auto'}
    spd_dic = {'01': 'LOW', '02': 'MEDIUM', '03': 'HIGH'}

    state = mode_dic.get(value[2:4]) if value[:2] == '10' else 'off'
    fan = spd_dic.get(value[4:6])
    temperature = int(value[8:10], 16)
    target = int(value[10:12], 16)

    logtxt = '[MQTT Parse | Ac] value[{}], state[{}], fan[{}], temp[{}], target[{}]'.format(
        value, state, fan, temperature, target)
    if logtxt != '' and config.get('Log', 'show_recv_hex') == 'True':
        logging.info(logtxt)
    return {'state': state, 'fan': fan, 'temperature': temperature, 'target': target}


# ── 삼성 AC 이중 패킷 병합 ─────────────────────────────────────────────────────

def merge_ac_state(state1, state2):
    """
    삼성 AC의 두 상태 패킷을 병합합니다.
    두 번째 패킷(state2)을 기준으로 하되,
    state2에 없거나 0/'off'/None인 값은 state1의 값으로 보완합니다.
    예: 첫 번째 패킷에 온도=0, 두 번째에 온도=24 → 24 사용
        첫 번째 패킷에 팬=LOW, 두 번째에 팬=None → LOW 사용
    """
    merged = {}
    all_keys = set(list(state1.keys()) + list(state2.keys()))
    for key in all_keys:
        v1 = state1.get(key)
        v2 = state2.get(key)
        # 두 번째 패킷 값이 유효하면 우선 사용
        if v2 is not None and v2 != 0 and v2 != 'off':
            merged[key] = v2
        # 첫 번째 패킷 값이 유효하면 보완
        elif v1 is not None and v1 != 0 and v1 != 'off':
            merged[key] = v1
        else:
            merged[key] = v2  # 둘 다 비유효면 두 번째 값 사용
    return merged


def publish_ac_state(room_id, state):
    """AC 상태를 MQTT로 발행합니다."""
    logtxt = '[MQTT publish|ac] id[{}] data[{}]'.format(room_id, state)
    mqttc.publish('kocom/room/ac/' + room_id + '/state', json.dumps(state), retain=True)
    if logtxt != '' and config.get('Log', 'show_mqtt_publish') == 'True':
        logging.info(logtxt)


def ac_packet_handler():
    """
    삼성 AC 이중 패킷 처리 전용 스레드입니다.

    삼성 AC는 하나의 명령/쿼리에 대해 상태 패킷을 2번 전송합니다.
    이 함수는 AC_DUAL_PACKET_TIMEOUT 이내에 도착한 두 패킷을 병합하여 발행합니다.
    단일 패킷만 오는 경우에는 타임아웃 후 그대로 발행합니다.

    AC_DUAL_PACKET_TIMEOUT = 0이면 즉시 발행(이중 패킷 처리 비활성화)합니다.
    """
    pending = {}  # room_id -> (state_dict, timestamp)
    poll_interval = max(0.05, AC_DUAL_PACKET_TIMEOUT / 4)

    while True:
        try:
            room_id, state = ac_packet_queue.get(True, poll_interval)
        except queue.Empty:
            # 타임아웃 초과한 미발행 패킷 처리
            now = time.time()
            expired = [rid for rid, (s, t) in list(pending.items())
                       if now - t >= AC_DUAL_PACKET_TIMEOUT]
            for rid in expired:
                s, _ = pending.pop(rid)
                logging.debug('[AC] Single packet timeout - publishing room {}: {}'.format(rid, s))
                publish_ac_state(rid, s)
            continue

        if AC_DUAL_PACKET_TIMEOUT == 0:
            # 이중 패킷 처리 비활성화: 즉시 발행
            publish_ac_state(room_id, state)
            continue

        if room_id in pending:
            prev_state, prev_time = pending.pop(room_id)
            elapsed = time.time() - prev_time
            if elapsed <= AC_DUAL_PACKET_TIMEOUT:
                # 두 번째 패킷 도착: 병합 후 발행
                merged = merge_ac_state(prev_state, state)
                logging.info('[AC] Dual-packet merged | room[{}] elapsed[{:.3f}s] | '
                             'pkt1[{}] + pkt2[{}] => [{}]'.format(
                                 room_id, elapsed, prev_state, state, merged))
                publish_ac_state(room_id, merged)
            else:
                # 타임아웃 초과: 이전 패킷 발행, 현재 패킷은 새로 버퍼링
                logging.debug('[AC] Second packet too late ({:.3f}s) - publishing first'.format(elapsed))
                publish_ac_state(room_id, prev_state)
                pending[room_id] = (state, time.time())
        else:
            # 첫 번째 패킷: 버퍼에 저장하고 두 번째 패킷 대기
            logging.debug('[AC] First packet buffered | room[{}] state[{}]'.format(room_id, state))
            pending[room_id] = (state, time.time())


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

    # [수정] 삼성 AC는 ACK value 검증 완화 (ack_value_check=False)
    is_ac = (device_h[:2] == device_h_dic['ac'])
    return send_wait_response(dest=device_h, cmd=cmd_h_dic['query'], log=log,
                              publish=publish, ack_value_check=not is_ac)


def send_wait_response(dest, src=device_h_dic['wallpad']+'00', cmd=cmd_h_dic['state'],
                       value='0'*16, log=None, check_ack=True, publish=True, ack_value_check=True):
    """
    ack_value_check: send()로 전달됩니다.
      True  = 기존 기기 (ACK value 완전 일치)
      False = 삼성 AC (ACK value 무시, 접두사만 비교)
    """
    #logging.debug('[**test1**]waiting for send_wait_response :'+dest)
    wait_target.put(dest)
    #logging.debug('[**test2**]entered send_wait_response :'+dest)
    ret = { 'value':'0'*16, 'flag':False }

    if send(dest, src, cmd, value, log, check_ack, ack_value_check) != False:
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

    # ── 삼성 에어컨 모드(켜기/끄기/모드변경) ────────────────────────────────────
    # kocom/room/ac/0/ac_mode/command  (command: off / cool / fan_only / dry / auto)
    elif 'ac' in topic_d and 'ac_mode' in topic_d:
        acmode_dic = {'off': '00', 'cool': '00', 'fan_only': '01', 'dry': '02', 'auto': '03'}
        dev_id = device_h_dic['ac'] + '{0:02x}'.format(int(topic_d[3]))

        if command == 'off':
            # 전원 끄기: 전체 0으로 설정
            value = '0000000000000000'
        else:
            # 전원 켜기 / 모드 변경: 현재 상태 읽어서 팬·온도 설정 보존
            is_on = '10'
            mode_byte = acmode_dic.get(command, '00')
            q = query(dev_id)
            if q['flag'] != False and q['value'] != '0' * 16:
                cur = q['value']
                # [0:2]=켜짐, [2:4]=새 모드, [4:]=기존 팬·온도 보존
                value = is_on + mode_byte + cur[4:]
                logging.info('[AC] ac_mode: preserving fan/temp from cache. '
                             'new value={}'.format(value))
            else:
                # 캐시 없으면 기본값 사용
                init_temp = '{0:02x}'.format(int(config.get('User', 'ac_init_temp', fallback='26')))
                value = is_on + mode_byte + '000000' + init_temp + '0000'
                logging.info('[AC] ac_mode: no cache, using defaults. value={}'.format(value))

        send_wait_response(dest=dev_id, value=value, log='ac mode', ack_value_check=False)

    # ── 삼성 에어컨 팬 속도 ────────────────────────────────────────────────────
    # kocom/room/ac/0/fan_mode/command  (command: LOW / MEDIUM / HIGH)
    elif 'ac' in topic_d and 'fan_mode' in topic_d:
        fan_dic = {'LOW': '01', 'MEDIUM': '02', 'HIGH': '03'}
        dev_id = device_h_dic['ac'] + '{0:02x}'.format(int(topic_d[3]))

        fan_byte = fan_dic.get(command, config.get('User', 'ac_init_fan_mode', fallback='01').strip())
        if fan_byte not in ('01', '02', '03'):
            fan_byte = '01'  # 잘못된 값이면 LOW로 fallback

        q = query(dev_id)
        if q['flag'] != False and q['value'] != '0' * 16:
            cur = q['value']
            # [0:4]=켜짐+모드 보존, [4:6]=새 팬속도, [6:]=나머지 보존
            value = cur[:4] + fan_byte + cur[6:]
            logging.info('[AC] fan_mode: preserving on/mode/temp from cache. '
                         'new value={}'.format(value))
        else:
            # 캐시 없으면 기본값: on + cool + 새 팬속도
            init_temp = '{0:02x}'.format(int(config.get('User', 'ac_init_temp', fallback='26')))
            value = '1000' + fan_byte + '00' + '00' + init_temp + '0000'
            logging.info('[AC] fan_mode: no cache, using defaults. value={}'.format(value))

        send_wait_response(dest=dev_id, value=value, log='ac fan_mode', ack_value_check=False)

    # ── 삼성 에어컨 설정 온도 ──────────────────────────────────────────────────
    # kocom/room/ac/0/set_temp/command
    elif 'ac' in topic_d and 'set_temp' in topic_d:
        dev_id = device_h_dic['ac'] + '{0:02x}'.format(int(topic_d[3]))
        settemp_hex = '{0:02x}'.format(int(float(command)))

        q = query(dev_id)
        if q['flag'] != False and q['value'] != '0' * 16:
            cur = q['value']
            # [0:10]=켜짐+모드+팬+여백 보존, [10:12]=새 설정온도, [12:]=나머지 보존
            value = cur[:10] + settemp_hex + cur[12:]
            logging.info('[AC] set_temp: preserving on/mode/fan from cache. '
                         'new value={}'.format(value))
        else:
            # 캐시 없으면 기본값: on + cool + LOW + 설정온도
            value = '1000' + '01' + '00' + '00' + settemp_hex + '0000'
            logging.info('[AC] set_temp: no cache, using defaults. value={}'.format(value))

        send_wait_response(dest=dev_id, value=value, log='ac settemp', ack_value_check=False)

    # light on/off : kocom/livingroom/light/1/command
    elif 'light' in topic_d:
        dev_id = device_h_dic['light'] + room_h_dic.get(topic_d[1])
        value = query(dev_id)['value']
        onoff_hex = 'ff' if command == 'on' else '00'
        light_id = int(topic_d[3])

        # turn on/off multiple lights at once : e.g) kocom/livingroom/light/12/command
        if light_id > 0:
            while light_id > 0:
                n = light_id % 10
                value = value[:n*2-2] + onoff_hex + value[n*2:]
                send_wait_response(dest=dev_id, value=value, log='light')
                light_id = int(light_id/10)
        else:
            send_wait_response(dest=dev_id, value=value, log='light')

    # gas off : kocom/livingroom/gas/command
    elif 'gas' in topic_d:
        dev_id = device_h_dic['gas'] + room_h_dic.get(topic_d[1])
        if command == 'off':
            send_wait_response(dest=dev_id, cmd=cmd_h_dic.get(command), log='gas')
        else:
            logging.info('You can only turn off gas.')

    # elevator on/off : kocom/myhome/elevator/command
    elif 'elevator' in topic_d:
        dev_id = device_h_dic['elevator'] + room_h_dic.get(topic_d[1])
        state_on = json.dumps({'state': 'on'})
        state_off = json.dumps({'state': 'off'})
        if command == 'on':
            ret_elevator = None
            if config.get('Elevator', 'type', fallback='rs485') == 'rs485':
                ret_elevator = send(dest=device_h_dic['wallpad']+'00', src=dev_id, cmd=cmd_h_dic['on'], value='0'*16, log='elevator', check_ack=False)
            elif config.get('Elevator', 'type', fallback='rs485') == 'tcpip':
                ret_elevator = call_elevator_tcpip()

            if ret_elevator == False:
                logging.debug('elevator send failed')
                return

            threading.Thread(target=mqttc.publish, args=("kocom/myhome/elevator/state", state_on)).start()
            if config.get('Elevator', 'rs485_floor', fallback=None) == None:
                threading.Timer(5, mqttc.publish, args=("kocom/myhome/elevator/state", state_off)).start()

        elif command == 'off':
            threading.Thread(target=mqttc.publish, args=("kocom/myhome/elevator/state", state_off)).start()

    # kocom/livingroom/fan/set_preset_mode/command
    elif 'fan' in topic_d and 'set_preset_mode' in topic_d:
        dev_id = device_h_dic['fan'] + room_h_dic.get(topic_d[1])
        onoff_dic = {'off':'0000', 'on':'1101'}
       #onoff_dic = {'off':'1000', 'on':'1100'}
        speed_dic = {'Off':'00', 'Low':'40', 'Medium':'80', 'High':'c0'}
        if command == 'Off':
            onoff = onoff_dic['off']
        elif command in speed_dic.keys(): # fan on with specified speed
            onoff = onoff_dic['on']

        speed = speed_dic.get(command)
        value = onoff + speed + '0'*10
        send_wait_response(dest=dev_id, value=value, log='fan')

    # kocom/livingroom/fan/command
    elif 'fan' in topic_d:
        dev_id = device_h_dic['fan'] + room_h_dic.get(topic_d[1])
        onoff_dic = {'off':'0000', 'on':'1101'}
       #onoff_dic = {'off':'1000', 'on':'1100'}
        speed_dic = {'Low':'40', 'Medium':'80', 'High':'c0'}
        init_fan_mode = config.get('User', 'init_fan_mode')
        if command in onoff_dic.keys(): # fan on off with previous speed
            onoff = onoff_dic.get(command)
            speed = speed_dic.get(init_fan_mode)  #value = query(dev_id)['value']  #speed = value[4:6]

        value = onoff + speed + '0'*10
        send_wait_response(dest=dev_id, value=value, log='fan')

    # kocom/myhome/query/command
    elif 'query' in topic_d:
        if command == 'PRESS':
            poll_state(enforce=True)


#===== parse hex packet --> publish MQTT =====

def publish_status(p):
    threading.Thread(target=packet_processor, args=(p,)).start()

def packet_processor(p):
    logtxt = ""
    if p['type'] == 'send' and p['dest'] == 'wallpad':  # response packet to wallpad
        if p['src'] == 'thermo' and p['cmd'] == 'state':
            state = thermo_parse(p['value'])
            logtxt='[MQTT publish|thermo] id[{}] data[{}]'.format(p['src_subid'], state)
            mqttc.publish("kocom/room/thermo/" + p['src_subid'] + "/state", json.dumps(state))
        elif p['src'] == 'ac' and p['cmd'] == 'state':
            # [수정] 삼성 AC 이중 패킷 처리: ac_packet_handler 스레드로 전달
            # ac_packet_handler가 두 패킷을 병합하여 MQTT 발행합니다.
            state = ac_parse(p['value'])
            ac_packet_queue.put((p['src_subid'], state))
            logtxt = '[AC] Queued packet for room[{}] state[{}]'.format(p['src_subid'], state)
        elif p['src'] == 'air':
            if int(p['value'], 16) > 0:
                state = air_parse(p['value'])
            logtxt = '[MQTT publish|air] data[{}]'.format(state)
            mqttc.publish('kocom/livingroom/air/state', json.dumps(state), retain=True)
        elif p['src'] == 'light' and p['cmd'] == 'state':
            state = light_parse(p['value'])
            logtxt='[MQTT publish|light] room[{}] data[{}]'.format(p['src_room'], state)
            mqttc.publish("kocom/{}/light/state".format(p['src_room']), json.dumps(state))
        elif p['src'] == 'fan' and p['cmd'] == 'state':
            state = fan_parse(p['value'])
            logtxt='[MQTT publish|fan] data[{}]'.format(state)
            mqttc.publish("kocom/livingroom/fan/state", json.dumps(state))
        elif p['src'] == 'gas':
            state = {'state': p['cmd']}
            logtxt='[MQTT publish|gas] data[{}]'.format(state)
            mqttc.publish("kocom/livingroom/gas/state", json.dumps(state))
    elif p['type'] == 'send' and p['dest'] == 'elevator':
        floor = int(p['value'][2:4],16)
        rs485_floor = int(config.get('Elevator','rs485_floor', fallback=0))
        if rs485_floor != 0 :
            state = {'floor': floor}
            if rs485_floor == floor:
                state['state'] = 'off'
        else:
            state = {'state': 'off'}
        logtxt='[MQTT publish|elevator] data[{}]'.format(state)
        mqttc.publish("kocom/myhome/elevator/state", json.dumps(state))
        # aa5530bc0044000100010300000000000000350d0d

    if logtxt != "" and config.get('Log', 'show_mqtt_publish') == 'True':
        logging.info(logtxt)


#===== publish MQTT Devices Discovery =====

def discovery():
    dev_list = [x.strip() for x in config.get('Device','enabled').split(',')]
    for t in dev_list:
        dev = t.split('_')
        sub = ''
        if len(dev) > 1:
            sub = dev[1]
        logtxt='[MQTT Discovery|{}] data[{}]'.format(dev[0], sub)
        publish_discovery(dev[0], sub)
        if logtxt != "" and config.get('Log', 'show_mqtt_discovery') == 'True':
            logging.info(logtxt)
    publish_discovery('query')

#https://www.home-assistant.io/docs/mqtt/discovery/
#<discovery_prefix>/<component>/<object_id>/config
def publish_discovery(dev, sub=''):
    if dev == 'fan':
        topic = 'homeassistant/fan/kocom_wallpad_fan/config'
        payload = {
            'name': 'Kocom Wallpad Fan',
            'cmd_t': 'kocom/livingroom/fan/command',
            'stat_t': 'kocom/livingroom/fan/state',
            'stat_val_tpl': '{{ value_json.state }}',
            'pr_mode_stat_t': 'kocom/livingroom/fan/state',
            'pr_mode_val_tpl': '{{ value_json.preset }}',
            'pr_mode_cmd_t': 'kocom/livingroom/fan/set_preset_mode/command',
            'pr_mode_cmd_tpl': '{{ value }}',
            'pr_modes': ['Off', 'Low', 'Medium', 'High'],
            'pl_on': 'on',
            'pl_off': 'off',
            'qos': 0,
            'uniq_id': '{}_{}_{}'.format('kocom', 'wallpad', dev),
            'device': {
                'name': '코콤 스마트 월패드',
                'ids': 'kocom_smart_wallpad',
                'mf': 'KOCOM',
                'mdl': '스마트 월패드',
                'sw': SW_VERSION
            }
        }
        logtxt='[MQTT Discovery|{}] data[{}]'.format(dev, topic)
        mqttc.publish(topic, json.dumps(payload))
        if logtxt != "" and config.get('Log', 'show_mqtt_publish') == 'True':
            logging.info(logtxt)
    elif dev == 'air':
        air_attr = {'pm10': ['molecule', 'µg/m³'], 'pm25': ['molecule', 'µg/m³'], 'co2': ['molecule-co2', 'ppm'], 'tvocs': ['molecule', 'ppb'], 'temperature': ['thermometer', '°C'], 'humidity': ['water-percent', '%'], 'score': ['periodic-table', '%']}
        for key, icon_unit in air_attr.items():
            icon, unit = icon_unit
            topic = f'homeassistant/sensor/kocom_wallpad_air_{key}/config'
            payload = {
                'name': f'kocom_air_{key}',
                'stat_t': 'kocom/livingroom/air/state',
                'val_tpl': '{{ value_json.' + key + ' }}',
                'qos': 0,
                'uniq_id': f'kocom_air_{key}',
                'icon': f'mdi:{icon}',
                'unit_of_meas': unit,
                'device': {
                    'name': '코콤 스마트 월패드',
                    'ids': 'kocom_smart_wallpad',
                    'mf': 'KOCOM',
                    'mdl': '스마트 월패드',
                    'sw': SW_VERSION
                }
            }
            logtxt = '[MQTT Discovery|{}] data[{}]'.format(dev, topic)
            mqttc.publish(topic, json.dumps(payload), retain=True)
            if logtxt != '' and config.get('Log', 'show_mqtt_publish') == 'True':
                logging.info(logtxt)
    elif dev == 'gas':
        topic = 'homeassistant/switch/kocom_wallpad_gas/config'
        payload = {
            'name': 'Kocom Wallpad Gas',
            'cmd_t': 'kocom/livingroom/gas/command',
            'stat_t': 'kocom/livingroom/gas/state',
            'val_tpl': '{{ value_json.state }}',
            'pl_on': 'on',
            'pl_off': 'off',
            'ic': 'mdi:gas-cylinder',
            'qos': 0,
            'uniq_id': '{}_{}_{}'.format('kocom', 'wallpad', dev),
            'device': {
                'name': '코콤 스마트 월패드',
                'ids': 'kocom_smart_wallpad',
                'mf': 'KOCOM',
                'mdl': '스마트 월패드',
                'sw': SW_VERSION
            }
        }
        logtxt='[MQTT Discovery|{}] data[{}]'.format(dev, topic)
        mqttc.publish(topic, json.dumps(payload))
        if logtxt != "" and config.get('Log', 'show_mqtt_publish') == 'True':
            logging.info(logtxt)
    elif dev == 'elevator':
        topic = 'homeassistant/switch/kocom_wallpad_elevator/config'
        payload = {
            'name': 'Kocom Wallpad Elevator',
            'cmd_t': "kocom/myhome/elevator/command",
            'stat_t': "kocom/myhome/elevator/state",
            'val_tpl': "{{ value_json.state }}",
            'pl_on': 'on',
            'pl_off': 'off',
            'ic': 'mdi:elevator',
            'qos': 0,
            'uniq_id': '{}_{}_{}'.format('kocom', 'wallpad', dev),
            'device': {
                'name': '코콤 스마트 월패드',
                'ids': 'kocom_smart_wallpad',
                'mf': 'KOCOM',
                'mdl': '스마트 월패드',
                'sw': SW_VERSION
            }
        }
        logtxt='[MQTT Discovery|{}] data[{}]'.format(dev, topic)
        mqttc.publish(topic, json.dumps(payload))
        if logtxt != "" and config.get('Log', 'show_mqtt_publish') == 'True':
            logging.info(logtxt)
    elif dev == 'light':

        for num in range(1, int(config.get('User', 'light_count'))+1):
            #ha_topic = 'homeassistant/light/kocom_livingroom_light1/config'
            topic = 'homeassistant/light/kocom_{}_light{}/config'.format(sub, num)
            payload = {
                'name': 'Kocom {} Light{}'.format(sub, num),
                'cmd_t': 'kocom/{}/light/{}/command'.format(sub, num),
                'stat_t': 'kocom/{}/light/state'.format(sub),
                'stat_val_tpl': '{{ value_json.light_' + str(num) + ' }}',
                'pl_on': 'on',
                'pl_off': 'off',
                'qos': 0,
#               'uniq_id': '{}_{}_{}{}'.format('kocom', 'wallpad', dev, num),      # 20221108 주석처리
                'uniq_id': '{}_{}_{}{}'.format('kocom', sub, dev, num),            # 20221108 수정

                'device': {
                    'name': '코콤 스마트 월패드',
                    'ids': 'kocom_smart_wallpad',
                    'mf': 'KOCOM',
                    'mdl': '스마트 월패드',
                    'sw': SW_VERSION
                }
            }
            logtxt='[MQTT Discovery|{}{}] data[{}]'.format(dev, num, topic)
            mqttc.publish(topic, json.dumps(payload))
            if logtxt != "" and config.get('Log', 'show_mqtt_publish') == 'True':
                logging.info(logtxt)
    elif dev == 'thermo':
        num = int(room_h_dic.get(sub))
        #ha_topic = 'homeassistant/climate/kocom_livingroom_thermostat/config'
        topic = 'homeassistant/climate/kocom_{}_thermostat/config'.format(sub)
        payload = {
            'name': 'Kocom {} Thermostat'.format(sub),
            'mode_cmd_t': 'kocom/room/thermo/{}/heat_mode/command'.format(num),
            'mode_stat_t': 'kocom/room/thermo/{}/state'.format(num),
            'mode_stat_tpl': '{{ value_json.heat_mode }}',

            'temp_cmd_t': 'kocom/room/thermo/{}/set_temp/command'.format(num),
            'temp_stat_t': 'kocom/room/thermo/{}/state'.format(num),
            'temp_stat_tpl': '{{ value_json.set_temp }}',

            'curr_temp_t': 'kocom/room/thermo/{}/state'.format(num),
            'curr_temp_tpl': '{{ value_json.cur_temp }}',
            'modes': ['off', 'heat'],
            'min_temp': 20,
            'max_temp': 30,
            'ret': 'false',
            'qos': 0,
            'uniq_id': '{}_{}_{}{}'.format('kocom', 'wallpad', dev, num),
            'device': {
                'name': '코콤 스마트 월패드',
                'ids': 'kocom_smart_wallpad',
                'mf': 'KOCOM',
                'mdl': '스마트 월패드',
                'sw': SW_VERSION
            }
        }
        logtxt='[MQTT Discovery|{}{}] data[{}]'.format(dev, num, topic)
        mqttc.publish(topic, json.dumps(payload))
        if logtxt != "" and config.get('Log', 'show_mqtt_publish') == 'True':
            logging.info(logtxt)
    elif dev == 'ac':
        num = int(room_h_dic.get(sub))
        # ha_topic = 'homeassistant/climate/kocom_livingroom_thermostat/config'
        topic = 'homeassistant/climate/kocom_{}_ac/config'.format(num)
        payload = {
            'name': 'kocom_ac_{}'.format(num),
            'mode_cmd_t': 'kocom/room/ac/{}/ac_mode/command'.format(num),
            'mode_stat_t': 'kocom/room/ac/{}/state'.format(num),
            'mode_stat_tpl': '{{ value_json.state }}',

            'fan_mode_cmd_t': 'kocom/room/ac/{}/fan_mode/command'.format(num),
            'fan_mode_stat_t': 'kocom/room/ac/{}/state'.format(num),
            'fan_mode_stat_tpl': '{{ value_json.fan }}',

            'temp_cmd_t': 'kocom/room/ac/{}/set_temp/command'.format(num),
            'temp_stat_t': 'kocom/room/ac/{}/state'.format(num),
            'temp_stat_tpl': '{{ value_json.target }}',

            'curr_temp_t': 'kocom/room/ac/{}/state'.format(num),
            'curr_temp_tpl': '{{ value_json.temperature }}',
            'modes': ['off', 'cool', 'fan_only', 'dry', 'auto'],
            'fan_modes': ['LOW', 'MEDIUM', 'HIGH'],
            'min_temp': 18,
            'max_temp': 30,
            'uniq_id': 'kocom_ac_{}'.format(num),
            'device': {
                'name': '코콤 스마트 월패드',
                'ids': 'kocom_smart_wallpad',
                'mf': 'KOCOM',
                'mdl': 'K스마트 월패드',
                'sw': SW_VERSION
            }
        }
        logtxt = '[MQTT Discovery|{}{}] data[{}]'.format(dev, sub, topic)
        mqttc.publish(topic, json.dumps(payload), retain=True)
        if logtxt != '' and config.get('Log', 'show_mqtt_publish') == 'True':
            logging.info(logtxt)
    elif dev == 'query':
        topic = 'homeassistant/button/kocom_wallpad_query/config'
        payload = {
            'name': 'Kocom Wallpad Query',
            'cmd_t': 'kocom/myhome/query/command',
            'qos': 0,
            'uniq_id': '{}_{}_{}'.format('kocom', 'wallpad', dev),
            'device': {
                'name': '코콤 스마트 월패드',
                'ids': 'kocom_smart_wallpad',
                'mf': 'KOCOM',
                'mdl': '스마트 월패드',
                'sw': SW_VERSION
            }
        }
        logtxt='[MQTT Discovery|{}] data[{}]'.format(dev, topic)
        mqttc.publish(topic, json.dumps(payload))
        if logtxt != "" and config.get('Log', 'show_mqtt_publish') == 'True':
            logging.info(logtxt)


#===== thread functions =====

def poll_state(enforce=False):
    global poll_timer
    poll_timer.cancel()

    dev_list = [x.strip() for x in config.get('Device','enabled').split(',')]
    no_polling_list = ['wallpad', 'elevator']

    #thread health check
    for thread_instance in thread_list:
        if thread_instance.is_alive() == False:
            logging.error('[THREAD] {} is not active. starting.'.format( thread_instance.name))
            thread_instance.start()

    for t in dev_list:
        dev = t.split('_')
        if dev[0] in no_polling_list:
            continue

        dev_id = device_h_dic.get(dev[0])
        if len(dev) > 1:
            sub_id = room_h_dic.get(dev[1])
        else:
            sub_id = '00'

        if dev_id != None and sub_id != None:
            if query(dev_id + sub_id, publish=True, enforce=enforce)['flag'] == False:
                break
            time.sleep(1)

    poll_timer.cancel()
    poll_timer = threading.Timer(polling_interval, poll_state)
    poll_timer.start()


def read_serial():
    global poll_timer
    buf = ''
    not_parsed_buf = ''
    while True:
        try:
            d = rs485.read()
            hex_d = '{0:02x}'.format(ord(d))

            buf += hex_d
            if buf[:len(header_h)] != header_h[:len(buf)]:
                not_parsed_buf += buf
                buf=''
                frame_start = not_parsed_buf.find(header_h, len(header_h))
                if frame_start < 0:
                    continue
                else:
                    not_parsed_buf = not_parsed_buf[:frame_start]
                    buf = not_parsed_buf[frame_start:]

            if not_parsed_buf != '':
                logging.info('[comm] not parsed '+not_parsed_buf)
                not_parsed_buf = ''


            if len(buf) == (packet_size * 2):
                chksum_calc = chksum(buf[len(header_h):chksum_position*2])
                chksum_buf = buf[chksum_position*2:chksum_position*2+2]
                if chksum_calc == chksum_buf and buf[-len(trailer_h):] == trailer_h:
                    if msg_q.full():
                        logging.error('msg_q is full. probably error occured while running listen_hexdata thread. please manually restart the program.')
                    msg_q.put(buf)  # valid packet
                    buf=''
                else:
                    logging.info("[comm] invalid packet {} expected checksum {}".format(buf, chksum_calc))
                    frame_start = buf.find(header_h, len(header_h))
                    # if there's header packet in the middle of invalid packet, re-parse from that posistion
                    if frame_start < 0:
                        not_parsed_buf += buf
                        buf=''
                    else:
                        not_parsed_buf += buf[:frame_start]
                        buf = buf[frame_start:]
        except Exception as ex:
            logging.error("*** Read error.[{}]".format(ex) )
            poll_timer.cancel()
            del cache_data[:]
            rs485.reconnect()
            poll_timer = threading.Timer(2, poll_state)
            poll_timer.start()


def listen_hexdata():
    while True:
        d = msg_q.get()

        if config.get('Log', 'show_recv_hex') == 'True':
            logging.info("[recv] " + d)

        p_ret = parse(d)

        # store recent packets in cache
        cache_data.insert(0, p_ret)
        if len(cache_data) > BUF_SIZE:
            del cache_data[-1]

        # [수정] ack_matches() 사용: 접두사(삼성 AC) 및 완전 일치(기존 기기) 모두 지원
        if ack_matches(p_ret['data_h'], ack_data):
            ack_q.put(d)
            continue

        if wait_target.empty() == False:
            if p_ret['dest_h'] == wait_target.queue[0] and p_ret['type'] == 'ack':
            #if p_ret['src_h'] == wait_target.queue[0] and p_ret['type'] == 'send':
                if len(ack_data) != 0:
                    logging.info("[ACK] No ack received, but responce packet received before ACK. Assuming ACK OK")
                    ack_q.put(d)
                    time.sleep(0.5)
                wait_q.put(p_ret)
                continue
        publish_status(p_ret)


#========== Main ==========

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
        logging.error('[CONFIG] invalid type value in [RS485]: only "serial" or "socket" is allowed. exit')
        exit(1)
    if rs485.connect() == False:
        logging.error('[RS485] connection error. exit')
        exit(1)

    mqttc = init_mqttc()
    if mqttc == False:
        logging.error('[MQTT] conection error. exit')
        exit(1)

    msg_q = queue.Queue(BUF_SIZE)
    ack_q = queue.Queue(1)
    ack_data = []
    wait_q = queue.Queue(1)
    wait_target = queue.Queue(1)
    send_lock = threading.Lock()
    poll_timer = threading.Timer(1, poll_state)

    cache_data = []

    # [추가] 삼성 AC 이중 패킷 처리용 큐
    ac_packet_queue = queue.Queue(50)

    thread_list = []
    thread_list.append(threading.Thread(target=read_serial, name='read_serial'))
    thread_list.append(threading.Thread(target=listen_hexdata, name='listen_hexdata'))
    # [추가] 삼성 AC 이중 패킷 처리 스레드 (daemon=True: 메인 종료 시 자동 종료)
    thread_list.append(threading.Thread(target=ac_packet_handler, name='ac_packet_handler', daemon=True))
    for thread_instance in thread_list:
        thread_instance.start()

    poll_timer.start()

    discovery()
