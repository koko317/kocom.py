#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
 python kocom script

 : forked from script written by vifrost, kyet, 룰루해피, 따분, Susu Daddy, harwin1
 : Samsung AC RS485 dual-packet support added

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


# ═══════════════════════════════════════════════════════════════════
#  기본 상수
# ═══════════════════════════════════════════════════════════════════
SW_VERSION = '2024.08.06'
CONFIG_FILE = 'kocom.conf'
BUF_SIZE = 100

read_write_gap   = 0.03   # 마지막 읽기 후 쓰기까지 최소 간격(초)
polling_interval = 300    # 폴링 주기(초)

header_h        = 'aa55'
trailer_h       = '0d0d'
packet_size     = 21      # 전체 21바이트
chksum_position = 18      # 체크섬 위치(18번째 바이트)

type_t_dic   = {'30b': 'send', '30d': 'ack'}
seq_t_dic    = {'c': 1, 'd': 2, 'e': 3, 'f': 4}
device_t_dic = {
    '01': 'wallpad', '0e': 'light', '2c': 'gas',
    '36': 'thermo',  '39': 'ac',   '3b': 'plug',
    '44': 'elevator','48': 'fan',  '98': 'air'
}
cmd_t_dic  = {'00': 'state', '01': 'on', '02': 'off', '3a': 'query'}
room_t_dic = {'00': 'livingroom', '01': 'room1', '02': 'room2',
               '03': 'room3',     '04': 'kitchen'}

type_h_dic   = {v: k for k, v in type_t_dic.items()}
seq_h_dic    = {v: k for k, v in seq_t_dic.items()}
device_h_dic = {v: k for k, v in device_t_dic.items()}
cmd_h_dic    = {v: k for k, v in cmd_t_dic.items()}
room_h_dic   = {
    'livingroom': '00', 'myhome': '00', 'room1': '01',
    'room2': '02',      'room3': '03',  'kitchen': '04'
}


# ═══════════════════════════════════════════════════════════════════
#  삼성 에어컨 RS485 프로토콜 설정
# ═══════════════════════════════════════════════════════════════════
#
#  [핵심 개념]
#  삼성 AC는 쿼리/명령 1회에 상태 패킷을 2번 전송합니다.
#
#    패킷① (즉시 응답): power=10, mode=00, fan=01,
#                       cur_temp=FF, set_temp=FF   ← 0xFF = 아직 준비 안 됨
#    패킷② (실제 상태): power=10, mode=00, fan=01,
#                       cur_temp=19, set_temp=18   ← 실제 온도값
#
#  두 패킷을 병합하되, 0xFF/범위초과 값은 반드시 걸러냅니다.
#  유효한 온도를 받지 못한 경우 마지막 유효값(last_good)을 사용합니다.
#
# ─── 타이밍 설정 ────────────────────────────────────────────────────
AC_DUAL_PACKET_TIMEOUT = 0.8   # 두 번째 패킷 대기 최대 시간(초)
                               # 0으로 설정 시 단일 패킷 모드

# ─── 온도 유효 범위 ──────────────────────────────────────────────────
AC_TEMP_MIN     = 10    # 유효 온도 최솟값 (°C)
AC_TEMP_MAX     = 40    # 유효 온도 최댓값 (°C)
AC_TEMP_INVALID = 0xFF  # 무효 마커 (255 = 삼성 AC "데이터 없음" 신호)

# ─── value 필드 바이트 위치 ──────────────────────────────────────────
#  KOCOM 패킷의 value 필드는 8바이트(16 hex chars)입니다.
#  삼성 AC 펌웨어 버전에 따라 온도 바이트 위치가 다를 수 있습니다.
#
#  255°C가 계속 표시된다면 아래 두 값을 조정하세요:
#    시도1) AC_BYTE_CURTEMP=3, AC_BYTE_SETTEMP=4
#    시도2) AC_BYTE_CURTEMP=5, AC_BYTE_SETTEMP=6
#
AC_BYTE_POWER   = 0   # 전원 (0x10=켜짐, 0x00=꺼짐)
AC_BYTE_MODE    = 1   # 운전 모드 (00=냉방, 01=팬, 02=제습, 03=자동, 04=난방)
AC_BYTE_FAN     = 2   # 팬 속도 (01=LOW, 02=MEDIUM, 03=HIGH)
AC_BYTE_EXTRA   = 3   # 예약/플래그
AC_BYTE_CURTEMP = 4   # 현재 실내 온도 ← 255가 나오면 3 또는 5로 변경 시도
AC_BYTE_SETTEMP = 5   # 목표 설정 온도 ← 255가 나오면 4 또는 6으로 변경 시도

# ─── 디버그 모드 ─────────────────────────────────────────────────────
#  True: 수신 패킷 전체 바이트를 로그로 출력 (바이트 위치 확인용)
#  온도 위치 파악 후 False로 변경하여 로그 줄이기 권장
AC_DEBUG_MODE = True


# ═══════════════════════════════════════════════════════════════════
#  MQTT
# ═══════════════════════════════════════════════════════════════════

def init_mqttc():
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    mqttc.on_message    = mqtt_on_message
    mqttc.on_subscribe  = mqtt_on_subscribe
    mqttc.on_connect    = mqtt_on_connect
    mqttc.on_disconnect = mqtt_on_disconnect

    if config.get('MQTT', 'mqtt_allow_anonymous') != 'True':
        logtxt = "[MQTT] connecting (using username and password)"
        mqttc.username_pw_set(
            username=config.get('MQTT', 'mqtt_username', fallback=''),
            password=config.get('MQTT', 'mqtt_password', fallback=''))
    else:
        logtxt = "[MQTT] connecting (anonymous)"

    mqtt_server = config.get('MQTT', 'mqtt_server')
    mqtt_port   = int(config.get('MQTT', 'mqtt_port'))
    for retry_cnt in range(1, 31):
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
    logging.info("[MQTT] on_log : " + string)

def mqtt_on_connect(mqttc, userdata, flags, rc):
    if rc == 0:
        logging.info("[MQTT] Connected - 0: OK")
        mqttc.subscribe('kocom/#', 0)
    else:
        logging.error("[MQTT] Connection error - {}: {}".format(rc, mqtt.connack_string(rc)))

def mqtt_on_disconnect(mqttc, userdata, rc=0):
    logging.error("[MQTT] Disconnected - " + str(rc))


# ═══════════════════════════════════════════════════════════════════
#  RS485 통신 래퍼
# ═══════════════════════════════════════════════════════════════════

class RS485Wrapper:
    def __init__(self, serial_port=None, socket_server=None, socket_port=0):
        if socket_server is None:
            self.type        = 'serial'
            self.serial_port = serial_port
        else:
            self.type          = 'socket'
            self.socket_server = socket_server
            self.socket_port   = socket_port
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
        if SERIAL_PORT is None:
            SERIAL_PORT = '/dev/ttyUSB0' if platform.system() == 'Linux' else 'com3'
        try:
            ser = serial.Serial(SERIAL_PORT, 9600, timeout=1)
            ser.bytesize = 8
            ser.stopbits = 1
            if not ser.is_open:
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
            logging.error('[RS485] Socket connection failure : {} | server {}, port {}'.format(
                e, SOCKET_SERVER, SOCKET_PORT))
            return False
        logging.info('[RS485] Socket connected | server {}, port {}'.format(
            SOCKET_SERVER, SOCKET_PORT))
        sock.settimeout(polling_interval + 15)
        return sock

    def read(self):
        if self.conn is False:
            return ''
        ret = ''
        if self.type == 'serial':
            for _ in range(polling_interval + 15):
                try:
                    ret = self.conn.read()
                except (AttributeError, TypeError):
                    raise Exception('exception occured while reading serial')
                if len(ret) != 0:
                    break
        elif self.type == 'socket':
            ret = self.conn.recv(1)
        if len(ret) == 0:
            raise Exception('read byte error')
        self.last_read_time = time.time()
        return ret

    def write(self, data):
        if self.conn is False:
            return False
        if self.last_read_time == 0:
            time.sleep(1)
        while time.time() - self.last_read_time < read_write_gap:
            time.sleep(max(0, read_write_gap - time.time() + self.last_read_time))
        if self.type == 'serial':
            return self.conn.write(data)
        elif self.type == 'socket':
            return self.conn.send(data)
        return False

    def close(self):
        if self.conn is not False:
            try:
                self.conn.close()
                self.conn = False
            except:
                pass
        return False

    def reconnect(self):
        self.close()
        while True:
            logging.info('[RS485] reconnecting to RS485...')
            if self.connect() is not False:
                break
            time.sleep(10)


# ═══════════════════════════════════════════════════════════════════
#  ACK 매칭 헬퍼
# ═══════════════════════════════════════════════════════════════════
#
#  기존 기기: ack_data에 32자 전체 패턴 → 완전 일치
#  삼성 AC  : ack_data에 14자 접두사만  → 시작 부분 일치
#             (AC의 ACK는 현재 상태값을 value에 실어 보내므로
#              보낸 value와 다르더라도 정상으로 처리)
#
def ack_matches(data_h, ack_patterns):
    """완전 일치 또는 접두사 일치를 모두 허용하는 ACK 매칭"""
    for pattern in ack_patterns:
        if data_h[:len(pattern)] == pattern:
            return True
    return False


# ═══════════════════════════════════════════════════════════════════
#  패킷 송신
# ═══════════════════════════════════════════════════════════════════

def send(dest, src, cmd, value, log=None, check_ack=True, ack_value_check=True):
    """
    RS485로 패킷을 전송합니다.

    ack_value_check=True  (기본, 기존 기기용):
        ACK 검증 시 type+seq+src+dest+cmd+value 완전 일치 필요
    ack_value_check=False (삼성 AC용):
        ACK 검증 시 type+seq+src+dest만 비교 (value 무시)
        삼성 AC는 ACK의 value 필드에 현재 전체 상태를 실어 보내므로
        보낸 value와 달라도 정상 ACK로 처리합니다.
    """
    send_lock.acquire()
    ack_data.clear()
    ret = False

    for seq_h in seq_t_dic.keys():
        payload   = type_h_dic['send'] + seq_h + '00' + dest + src + cmd + value
        send_data = header_h + payload + chksum(payload) + trailer_h
        try:
            if rs485.write(bytearray.fromhex(send_data)) is False:
                raise Exception('Not ready')
        except Exception as ex:
            logging.error("[RS485] Write error.[{}]".format(ex))
            break

        if log is not None:
            logging.info('[SEND|{}] {}'.format(log, send_data))

        if not check_ack:
            time.sleep(1)
            ret = send_data
            break

        # ACK 패턴 등록
        if ack_value_check:
            # 기존 방식: value까지 완전 일치
            ack_data.append(type_h_dic['ack'] + seq_h + '00' + src + dest + cmd + value)
        else:
            # 삼성 AC: type+seq+src+dest 접두사만 (value 무시)
            ack_data.append(type_h_dic['ack'] + seq_h + '00' + src + dest)

        try:
            ack_q.get(True, 1.3 + 0.2 * random.random())
            if config.get('Log', 'show_recv_hex') == 'True':
                logging.info('[ACK] OK')
            ret = send_data
            break
        except queue.Empty:
            pass

    if ret is False:
        logging.info('[RS485] send failed. closing RS485. will reconnect shortly.')
        rs485.close()

    ack_data.clear()
    send_lock.release()
    return ret


def chksum(data_h):
    return '{0:02x}'.format(sum(bytearray.fromhex(data_h)) % 256)


# ═══════════════════════════════════════════════════════════════════
#  패킷 파싱 (공통)
# ═══════════════════════════════════════════════════════════════════

def parse(hex_data):
    header_h_  = hex_data[:4]
    type_h     = hex_data[4:7]
    seq_h      = hex_data[7:8]
    monitor_h  = hex_data[8:10]
    dest_h     = hex_data[10:14]
    src_h      = hex_data[14:18]
    cmd_h      = hex_data[18:20]
    value_h    = hex_data[20:36]
    chksum_h   = hex_data[36:38]
    trailer_h_ = hex_data[38:42]

    data_h    = hex_data[4:36]
    payload_h = hex_data[18:36]
    cmd       = cmd_t_dic.get(cmd_h)

    return {
        'header_h':  header_h_,  'type_h':   type_h,   'seq_h':    seq_h,
        'monitor_h': monitor_h,  'dest_h':   dest_h,   'src_h':    src_h,
        'cmd_h':     cmd_h,      'value_h':  value_h,  'chksum_h': chksum_h,
        'trailer_h': trailer_h_, 'data_h':   data_h,   'payload_h':payload_h,
        'type':      type_t_dic.get(type_h),
        'seq':       seq_t_dic.get(seq_h),
        'dest':      device_t_dic.get(dest_h[:2]),
        'dest_subid':str(int(dest_h[2:4], 16)),
        'dest_room': room_t_dic.get(dest_h[2:4]),
        'src':       device_t_dic.get(src_h[:2]),
        'src_subid': str(int(src_h[2:4], 16)),
        'src_room':  room_t_dic.get(src_h[2:4]),
        'cmd':       cmd if cmd is not None else cmd_h,
        'value':     value_h,
        'time':      time.time(),
        'flag':      None
    }


def thermo_parse(value):
    return {
        'heat_mode': 'heat' if value[:2] == '11' else 'off',
        'away':      'true' if value[2:4] == '01' else 'false',
        'set_temp':  int(value[4:6], 16) if value[:2] == '11'
                     else int(config.get('User', 'init_temp')),
        'cur_temp':  int(value[8:10], 16)
    }


def light_parse(value):
    ret = {}
    for i in range(1, int(config.get('User', 'light_count')) + 1):
        ret['light_' + str(i)] = 'off' if value[i*2-2:i*2] == '00' else 'on'
    return ret


def fan_parse(value):
    preset_dic = {'40': 'Low', '80': 'Medium', 'c0': 'High'}
    state  = 'off' if value[:2] == '00' else 'on'
    preset = 'Off' if state == 'off' else preset_dic.get(value[4:6])
    logtxt = '[MQTT Parse | Fan] value[{}], state[{}]'.format(value, state)
    if logtxt and config.get('Log', 'show_recv_hex') == 'True':
        logging.info(logtxt)
    return {'state': state, 'preset': preset}


# ═══════════════════════════════════════════════════════════════════
#  삼성 에어컨 파싱 & 상태 관리
# ═══════════════════════════════════════════════════════════════════

# 마지막으로 확인된 유효 상태 (room_id → state_dict)
# 0xFF 등 무효값이 들어올 때 이 캐시로 보완합니다.
ac_last_good_state = {}


def is_valid_ac_temp(val):
    """
    온도값이 유효한지 검사합니다.
      - None         → 무효
      - 0xFF (255)   → 무효 (삼성 AC "데이터 없음" 마커)
      - 범위 초과     → 무효 (AC_TEMP_MIN ~ AC_TEMP_MAX)
    """
    if val is None:
        return False
    if val == AC_TEMP_INVALID:   # 255
        return False
    return AC_TEMP_MIN <= val <= AC_TEMP_MAX


def ac_parse(value):
    """
    삼성 에어컨 상태 패킷 파싱 (value 필드 8바이트 = 16 hex chars)

    반환값:
      state       : 'cool'|'fan_only'|'dry'|'auto'|'heat'|'off'
      fan         : 'LOW'|'MEDIUM'|'HIGH'|None
      temperature : 현재 실내 온도(int) 또는 None (0xFF/범위초과 시)
      target      : 목표 설정 온도(int) 또는 None (0xFF/범위초과 시)

    ※ temperature/target 가 None 이면 해당 패킷에서 온도를 읽지 못한 것입니다.
       ac_packet_handler가 두 패킷을 합산하거나 last_good_state로 보완합니다.
    """
    mode_dic = {
        0x00: 'cool', 0x01: 'fan_only', 0x02: 'dry',
        0x03: 'auto', 0x04: 'heat'
    }
    spd_dic = {0x01: 'LOW', 0x02: 'MEDIUM', 0x03: 'HIGH'}

    try:
        raw = bytes.fromhex(value)
    except Exception as e:
        logging.error('[AC] ac_parse: hex 변환 실패 value={} err={}'.format(value, e))
        return None

    if len(raw) < 6:
        logging.error('[AC] ac_parse: value가 너무 짧음 ({} bytes)'.format(len(raw)))
        return None

    # ── 디버그 로깅: 바이트 위치를 육안으로 확인 ──────────────────────
    if AC_DEBUG_MODE:
        byte_info = '  '.join(
            'B{}={:02X}({})'.format(i, b, b) for i, b in enumerate(raw)
        )
        logging.info('[AC DEBUG] 수신 value={} → {}'.format(value, byte_info))
        logging.info('[AC DEBUG] 참고: 현재 온도 읽는 위치 → '
                     'cur_temp=B{}={:02X}({})  set_temp=B{}={:02X}({})'.format(
                         AC_BYTE_CURTEMP, raw[AC_BYTE_CURTEMP], raw[AC_BYTE_CURTEMP],
                         AC_BYTE_SETTEMP, raw[AC_BYTE_SETTEMP], raw[AC_BYTE_SETTEMP]))
        logging.info('[AC DEBUG] 만약 온도가 255로 표시되면 kocom.py 상단의 '
                     'AC_BYTE_CURTEMP / AC_BYTE_SETTEMP 값을 조정하세요.')

    power_byte   = raw[AC_BYTE_POWER]
    mode_byte    = raw[AC_BYTE_MODE]
    fan_byte     = raw[AC_BYTE_FAN]
    curtemp_byte = raw[AC_BYTE_CURTEMP] if AC_BYTE_CURTEMP < len(raw) else AC_TEMP_INVALID
    settemp_byte = raw[AC_BYTE_SETTEMP] if AC_BYTE_SETTEMP < len(raw) else AC_TEMP_INVALID

    state = mode_dic.get(mode_byte) if power_byte == 0x10 else 'off'
    fan   = spd_dic.get(fan_byte)

    # 0xFF(255) 또는 범위 초과는 None으로 처리
    temperature = curtemp_byte if is_valid_ac_temp(curtemp_byte) else None
    target      = settemp_byte if is_valid_ac_temp(settemp_byte) else None

    result = {
        'state': state, 'fan': fan,
        'temperature': temperature, 'target': target
    }

    logtxt = ('[AC] 파싱결과 value={} → state={}, fan={}, '
              'cur_temp={}{}, set_temp={}{}'.format(
                  value, state, fan,
                  temperature, '' if is_valid_ac_temp(temperature) else '(무효)',
                  target,      '' if is_valid_ac_temp(target)      else '(무효)'))
    if config.get('Log', 'show_recv_hex') == 'True':
        logging.info(logtxt)

    return result


def merge_ac_state(state1, state2):
    """
    두 AC 상태 패킷을 병합합니다.

    우선순위 규칙:
      온도 필드 (temperature, target):
        is_valid_ac_temp()를 통과한 값만 사용
        state2 먼저, 없으면 state1 사용
        둘 다 무효면 None 유지 (publish_ac_state에서 last_good로 보완)

      상태 필드 (state, fan):
        state2 먼저, 없으면 state1 사용
        단, 'off'와 None만 있을 경우 state1 우선
    """
    merged = {}

    # 온도 필드: 유효성 검사 필수
    for key in ('temperature', 'target'):
        v2 = state2.get(key) if state2 else None
        v1 = state1.get(key) if state1 else None
        if is_valid_ac_temp(v2):
            merged[key] = v2
        elif is_valid_ac_temp(v1):
            merged[key] = v1
            if AC_DEBUG_MODE:
                logging.info('[AC MERGE] {}: state2={} 무효 → state1={} 사용'.format(
                    key, v2, v1))
        else:
            merged[key] = None  # 둘 다 무효 → publish_ac_state에서 last_good 보완
            if AC_DEBUG_MODE:
                logging.info('[AC MERGE] {}: 양쪽 모두 무효(state1={}, state2={}) '
                             '→ last_good 사용 예정'.format(key, v1, v2))

    # 운전 상태·팬: 비유효값 우선순위
    for key in ('state', 'fan'):
        v2 = state2.get(key) if state2 else None
        v1 = state1.get(key) if state1 else None
        if v2 is not None and v2 != 'off':
            merged[key] = v2
        elif v1 is not None and v1 != 'off':
            merged[key] = v1
        else:
            merged[key] = v2  # 둘 다 off/None이면 state2 유지

    return merged


def publish_ac_state(room_id, state):
    """
    AC 상태를 MQTT로 발행합니다.

    온도 필드가 None(무효)이면 ac_last_good_state에서 마지막 유효값으로 보완합니다.
    최종적으로도 온도를 구할 수 없으면 해당 필드를 발행하지 않습니다.
    """
    global ac_last_good_state

    if state is None:
        return

    final = dict(state)

    # ── last_good_state로 무효 온도 보완 ──────────────────────────────
    last = ac_last_good_state.get(room_id, {})
    for key in ('temperature', 'target'):
        if not is_valid_ac_temp(final.get(key)):
            fallback = last.get(key)
            if is_valid_ac_temp(fallback):
                final[key] = fallback
                logging.info('[AC] room[{}] {} 무효 → last_good {} 사용'.format(
                    room_id, key, fallback))
            else:
                # 정말 알 수 없는 경우: 합리적인 기본값 (발행은 하되 0 방지)
                final[key] = None

    # None 제거 (HA가 None을 처리 못할 수 있음)
    final = {k: v for k, v in final.items() if v is not None}

    # ── last_good_state 갱신 (유효값만) ──────────────────────────────
    if room_id not in ac_last_good_state:
        ac_last_good_state[room_id] = {}
    for key, val in state.items():
        if key in ('temperature', 'target'):
            if is_valid_ac_temp(val):
                ac_last_good_state[room_id][key] = val
        elif val is not None and val != 'off':
            ac_last_good_state[room_id][key] = val

    logtxt = '[MQTT publish|ac] room[{}] → {}'.format(room_id, final)
    mqttc.publish('kocom/room/ac/' + room_id + '/state', json.dumps(final), retain=True)
    if config.get('Log', 'show_mqtt_publish') == 'True':
        logging.info(logtxt)


def ac_packet_handler():
    """
    삼성 AC 이중 패킷 처리 전용 스레드

    동작 흐름:
      1. packet_processor → ac_packet_queue.put((room_id, state))
      2. 이 스레드가 큐에서 꺼내 room별 버퍼에 저장
      3. AC_DUAL_PACKET_TIMEOUT 이내에 같은 room의 두 번째 패킷 도착
         → 두 패킷 merge_ac_state() 병합 → publish_ac_state()
      4. 두 번째 패킷이 오지 않으면 타임아웃 후 첫 번째 패킷만 발행

    AC_DUAL_PACKET_TIMEOUT = 0 이면 즉시 발행 (단일 패킷 모드)
    """
    pending   = {}   # room_id → (state_dict, timestamp)
    poll_wait = max(0.05, AC_DUAL_PACKET_TIMEOUT / 4)

    while True:
        # ── 큐에서 패킷 꺼내기 ─────────────────────────────────────────
        try:
            room_id, state = ac_packet_queue.get(True, poll_wait)
        except queue.Empty:
            # 타임아웃 초과된 버퍼 패킷 처리
            now = time.time()
            expired = [rid for rid, (s, t) in list(pending.items())
                       if now - t >= AC_DUAL_PACKET_TIMEOUT]
            for rid in expired:
                s, _ = pending.pop(rid)
                logging.debug('[AC] 단일 패킷 타임아웃 → 발행 room={}'.format(rid))
                publish_ac_state(rid, s)
            continue

        # ── 이중 패킷 처리 비활성화 시 즉시 발행 ──────────────────────
        if AC_DUAL_PACKET_TIMEOUT == 0:
            publish_ac_state(room_id, state)
            continue

        # ── 두 번째 패킷 도착 시 병합 ──────────────────────────────────
        if room_id in pending:
            prev_state, prev_time = pending.pop(room_id)
            elapsed = time.time() - prev_time

            if elapsed <= AC_DUAL_PACKET_TIMEOUT:
                merged = merge_ac_state(prev_state, state)
                logging.info(
                    '[AC] 이중패킷 병합 room={} elapsed={:.3f}s | '
                    'pkt1={} + pkt2={} → {}'.format(
                        room_id, elapsed, prev_state, state, merged))
                publish_ac_state(room_id, merged)
            else:
                # 너무 늦게 도착 → 이전 패킷 먼저 발행, 현재 패킷은 새로 버퍼링
                logging.debug('[AC] 두 번째 패킷 지연({:.3f}s) → 이전 패킷 단독 발행'.format(elapsed))
                publish_ac_state(room_id, prev_state)
                pending[room_id] = (state, time.time())
        else:
            # 첫 번째 패킷: 버퍼에 저장하고 두 번째 패킷 대기
            logging.debug('[AC] 첫 번째 패킷 버퍼링 room={} state={}'.format(room_id, state))
            pending[room_id] = (state, time.time())


# ═══════════════════════════════════════════════════════════════════
#  쿼리 & 송신 대기
# ═══════════════════════════════════════════════════════════════════

def query(device_h, publish=False, enforce=False):
    # 캐시에서 먼저 확인
    for c in cache_data:
        if enforce:
            break
        if time.time() - c['time'] > polling_interval:
            break
        if (c['type'] == 'ack' and c['src'] == 'wallpad'
                and c['dest_h'] == device_h and c['cmd'] != 'query'):
            if config.get('Log', 'show_query_hex') == 'True':
                logging.info('[cache|{}{}] {}'.format(
                    c['dest'], c['dest_subid'], c['data_h']))
            return c

    log = ('query ' + device_t_dic.get(device_h[:2]) + str(int(device_h[2:4], 16))
           if config.get('Log', 'show_query_hex') == 'True' else None)

    # 삼성 AC는 ACK value 검증 완화
    is_ac = (device_h[:2] == device_h_dic['ac'])
    return send_wait_response(dest=device_h, cmd=cmd_h_dic['query'],
                              log=log, publish=publish,
                              ack_value_check=not is_ac)


def send_wait_response(dest, src=None, cmd=None, value='0'*16,
                       log=None, check_ack=True, publish=True, ack_value_check=True):
    if src is None:
        src = device_h_dic['wallpad'] + '00'
    if cmd is None:
        cmd = cmd_h_dic['state']

    wait_target.put(dest)
    ret = {'value': '0'*16, 'flag': False}

    if send(dest, src, cmd, value, log, check_ack, ack_value_check) is not False:
        try:
            ret = wait_q.get(True, 2)
            if publish:
                publish_status(ret)
        except queue.Empty:
            pass

    wait_target.get()
    return ret


# ═══════════════════════════════════════════════════════════════════
#  엘리베이터 TCP/IP 호출
# ═══════════════════════════════════════════════════════════════════

def call_elevator_tcpip():
    import socket as _socket
    sock = _socket.socket()
    sock.settimeout(10)

    APT_SERVER = config.get('Elevator', 'tcpip_apt_server')
    APT_PORT   = int(config.get('Elevator', 'tcpip_apt_port'))

    try:
        sock.connect((APT_SERVER, APT_PORT))
    except Exception as e:
        logging.error('Elevator TCP connection failure: {}'.format(e))
        return False
    logging.info('Elevator TCP connected: {}:{}'.format(APT_SERVER, APT_PORT))

    try:
        sock.send(bytearray.fromhex(config.get('Elevator', 'tcpip_packet1')))
        logging.info('recv: ' + ''.join('%02x' % i for i in sock.recv(512)))
        time.sleep(0.1)
        sock.send(bytearray.fromhex(config.get('Elevator', 'tcpip_packet2')))
        logging.info('recv: ' + ''.join('%02x' % i for i in sock.recv(512)))
        sock.send(bytearray.fromhex(config.get('Elevator', 'tcpip_packet3')))
        for _ in range(100):
            rcv = sock.recv(512)
            if not rcv:
                sock.close()
                return True
            rcv_hex = ''.join('%02x' % i for i in rcv)
            logging.info('recv: ' + rcv_hex)
            if rcv_hex == config.get('Elevator', 'tcpip_packet4'):
                break
        sock.send(bytearray.fromhex(config.get('Elevator', 'tcpip_packet2')))
        logging.info('recv: ' + ''.join('%02x' % i for i in sock.recv(512)))
        sock.close()
    except Exception as e:
        logging.error('Elevator TCP comm failure: {}'.format(e))
        return False
    return True


# ═══════════════════════════════════════════════════════════════════
#  MQTT 수신 → RS485 명령 변환
# ═══════════════════════════════════════════════════════════════════

def mqtt_on_message(mqttc, obj, msg):
    command = msg.payload.decode('ascii')
    topic_d = msg.topic.split('/')

    if topic_d[-1] != 'command':
        return

    logging.info("[MQTT RECV] {} {} {}".format(msg.topic, msg.qos, msg.payload))

    # ── 온도조절기 ────────────────────────────────────────────────────
    if 'thermo' in topic_d and 'heat_mode' in topic_d:
        heatmode_dic = {'heat': '11', 'off': '00'}
        dev_id = device_h_dic['thermo'] + '{:02x}'.format(int(topic_d[3]))
        q = query(dev_id)
        settemp_hex = '{:02x}'.format(
            int(config.get('User', 'thermo_init_temp'))) if q['flag'] is not False else '14'
        value = heatmode_dic.get(command) + '00' + settemp_hex + '0000000000'
        send_wait_response(dest=dev_id, value=value, log='thermo heatmode')

    elif 'thermo' in topic_d and 'set_temp' in topic_d:
        dev_id = device_h_dic['thermo'] + '{:02x}'.format(int(topic_d[3]))
        value  = '1100' + '{:02x}'.format(int(float(command))) + '0000000000'
        send_wait_response(dest=dev_id, value=value, log='thermo settemp')

    # ── 삼성 에어컨: 전원/모드 ───────────────────────────────────────
    # topic: kocom/room/ac/{num}/ac_mode/command
    # command: off | cool | fan_only | dry | auto | heat
    elif 'ac' in topic_d and 'ac_mode' in topic_d:
        acmode_dic = {'off': 0x00, 'cool': 0x00, 'fan_only': 0x01,
                      'dry': 0x02, 'auto': 0x03, 'heat': 0x04}
        dev_id = device_h_dic['ac'] + '{:02x}'.format(int(topic_d[3]))

        if command == 'off':
            value = '00' * 8   # 전원 끄기: 전체 0
        else:
            mode_byte = acmode_dic.get(command, 0x00)
            q = query(dev_id)
            if q['flag'] is not False and q['value'] != '0' * 16:
                cur = bytes.fromhex(q['value'])
                raw = bytearray(cur)
                raw[AC_BYTE_POWER] = 0x10
                raw[AC_BYTE_MODE]  = mode_byte
                value = raw.hex()
                logging.info('[AC] ac_mode: 캐시에서 팬·온도 보존 value={}'.format(value))
            else:
                init_temp = int(config.get('User', 'ac_init_temp', fallback='26'))
                raw = bytearray(8)
                raw[AC_BYTE_POWER]   = 0x10
                raw[AC_BYTE_MODE]    = mode_byte
                raw[AC_BYTE_FAN]     = 0x01        # LOW
                raw[AC_BYTE_SETTEMP] = init_temp
                value = raw.hex()
                logging.info('[AC] ac_mode: 기본값 사용 value={}'.format(value))

        send_wait_response(dest=dev_id, value=value,
                           log='ac mode', ack_value_check=False)

    # ── 삼성 에어컨: 팬 속도 ─────────────────────────────────────────
    # topic: kocom/room/ac/{num}/fan_mode/command
    # command: LOW | MEDIUM | HIGH
    elif 'ac' in topic_d and 'fan_mode' in topic_d:
        fan_dic = {'LOW': 0x01, 'MEDIUM': 0x02, 'HIGH': 0x03}
        dev_id   = device_h_dic['ac'] + '{:02x}'.format(int(topic_d[3]))
        fan_byte = fan_dic.get(command, 0x01)

        q = query(dev_id)
        if q['flag'] is not False and q['value'] != '0' * 16:
            raw = bytearray(bytes.fromhex(q['value']))
            raw[AC_BYTE_FAN] = fan_byte
            value = raw.hex()
            logging.info('[AC] fan_mode: 캐시에서 모드·온도 보존 value={}'.format(value))
        else:
            init_temp = int(config.get('User', 'ac_init_temp', fallback='26'))
            raw = bytearray(8)
            raw[AC_BYTE_POWER]   = 0x10
            raw[AC_BYTE_FAN]     = fan_byte
            raw[AC_BYTE_SETTEMP] = init_temp
            value = raw.hex()
            logging.info('[AC] fan_mode: 기본값 사용 value={}'.format(value))

        send_wait_response(dest=dev_id, value=value,
                           log='ac fan_mode', ack_value_check=False)

    # ── 삼성 에어컨: 설정 온도 ───────────────────────────────────────
    # topic: kocom/room/ac/{num}/set_temp/command
    elif 'ac' in topic_d and 'set_temp' in topic_d:
        dev_id   = device_h_dic['ac'] + '{:02x}'.format(int(topic_d[3]))
        set_temp = int(float(command))

        q = query(dev_id)
        if q['flag'] is not False and q['value'] != '0' * 16:
            raw = bytearray(bytes.fromhex(q['value']))
            raw[AC_BYTE_SETTEMP] = set_temp
            value = raw.hex()
            logging.info('[AC] set_temp: 캐시에서 모드·팬 보존 value={}'.format(value))
        else:
            raw = bytearray(8)
            raw[AC_BYTE_POWER]   = 0x10
            raw[AC_BYTE_FAN]     = 0x01
            raw[AC_BYTE_SETTEMP] = set_temp
            value = raw.hex()
            logging.info('[AC] set_temp: 기본값 사용 value={}'.format(value))

        send_wait_response(dest=dev_id, value=value,
                           log='ac settemp', ack_value_check=False)

    # ── 조명 ──────────────────────────────────────────────────────────
    elif 'light' in topic_d:
        dev_id    = device_h_dic['light'] + room_h_dic.get(topic_d[1])
        value     = query(dev_id)['value']
        onoff_hex = 'ff' if command == 'on' else '00'
        light_id  = int(topic_d[3])
        if light_id > 0:
            while light_id > 0:
                n     = light_id % 10
                value = value[:n*2-2] + onoff_hex + value[n*2:]
                send_wait_response(dest=dev_id, value=value, log='light')
                light_id = int(light_id / 10)
        else:
            send_wait_response(dest=dev_id, value=value, log='light')

    # ── 가스 ──────────────────────────────────────────────────────────
    elif 'gas' in topic_d:
        dev_id = device_h_dic['gas'] + room_h_dic.get(topic_d[1])
        if command == 'off':
            send_wait_response(dest=dev_id, cmd=cmd_h_dic.get(command), log='gas')
        else:
            logging.info('가스는 끄기만 가능합니다.')

    # ── 엘리베이터 ───────────────────────────────────────────────────
    elif 'elevator' in topic_d:
        dev_id    = device_h_dic['elevator'] + room_h_dic.get(topic_d[1])
        state_on  = json.dumps({'state': 'on'})
        state_off = json.dumps({'state': 'off'})
        if command == 'on':
            ret_el = None
            if config.get('Elevator', 'type', fallback='rs485') == 'rs485':
                ret_el = send(dest=device_h_dic['wallpad']+'00', src=dev_id,
                              cmd=cmd_h_dic['on'], value='0'*16,
                              log='elevator', check_ack=False)
            elif config.get('Elevator', 'type', fallback='rs485') == 'tcpip':
                ret_el = call_elevator_tcpip()
            if ret_el is False:
                return
            threading.Thread(
                target=mqttc.publish,
                args=("kocom/myhome/elevator/state", state_on)).start()
            if config.get('Elevator', 'rs485_floor', fallback=None) is None:
                threading.Timer(
                    5, mqttc.publish,
                    args=("kocom/myhome/elevator/state", state_off)).start()
        elif command == 'off':
            threading.Thread(
                target=mqttc.publish,
                args=("kocom/myhome/elevator/state", state_off)).start()

    # ── 환풍기 (프리셋) ──────────────────────────────────────────────
    elif 'fan' in topic_d and 'set_preset_mode' in topic_d:
        dev_id    = device_h_dic['fan'] + room_h_dic.get(topic_d[1])
        onoff_dic = {'off': '0000', 'on': '1101'}
        speed_dic = {'Off': '00', 'Low': '40', 'Medium': '80', 'High': 'c0'}
        onoff = onoff_dic['off'] if command == 'Off' else onoff_dic['on']
        speed = speed_dic.get(command, '00')
        value = onoff + speed + '0' * 10
        send_wait_response(dest=dev_id, value=value, log='fan preset')

    # ── 환풍기 (켜기/끄기) ───────────────────────────────────────────
    elif 'fan' in topic_d:
        dev_id    = device_h_dic['fan'] + room_h_dic.get(topic_d[1])
        onoff_dic = {'off': '0000', 'on': '1101'}
        speed_dic = {'Low': '40', 'Medium': '80', 'High': 'c0'}
        onoff = onoff_dic.get(command, '0000')
        speed = speed_dic.get(config.get('User', 'init_fan_mode'), '40')
        value = onoff + speed + '0' * 10
        send_wait_response(dest=dev_id, value=value, log='fan')

    # ── 수동 쿼리 ────────────────────────────────────────────────────
    elif 'query' in topic_d:
        if command == 'PRESS':
            poll_state(enforce=True)


# ═══════════════════════════════════════════════════════════════════
#  수신 패킷 → MQTT 발행
# ═══════════════════════════════════════════════════════════════════

def publish_status(p):
    threading.Thread(target=packet_processor, args=(p,)).start()


def packet_processor(p):
    logtxt = ''

    if p['type'] == 'send' and p['dest'] == 'wallpad':
        if p['src'] == 'thermo' and p['cmd'] == 'state':
            state  = thermo_parse(p['value'])
            logtxt = '[MQTT publish|thermo] id={} {}'.format(p['src_subid'], state)
            mqttc.publish("kocom/room/thermo/" + p['src_subid'] + "/state",
                          json.dumps(state))

        elif p['src'] == 'ac' and p['cmd'] == 'state':
            # ── 삼성 AC: ac_packet_handler 스레드로 전달 ─────────────
            # (이중 패킷 병합 + 0xFF 필터링 + last_good 보완은
            #  ac_packet_handler / publish_ac_state 에서 처리)
            state = ac_parse(p['value'])
            if state is not None:
                ac_packet_queue.put((p['src_subid'], state))
                logtxt = '[AC] 큐 전달 room={} raw={}'.format(
                    p['src_subid'], p['value'])

        elif p['src'] == 'air':
            if int(p['value'], 16) > 0:
                state = air_parse(p['value'])
            logtxt = '[MQTT publish|air] {}'.format(state)
            mqttc.publish('kocom/livingroom/air/state', json.dumps(state), retain=True)

        elif p['src'] == 'light' and p['cmd'] == 'state':
            state  = light_parse(p['value'])
            logtxt = '[MQTT publish|light] room={} {}'.format(p['src_room'], state)
            mqttc.publish("kocom/{}/light/state".format(p['src_room']), json.dumps(state))

        elif p['src'] == 'fan' and p['cmd'] == 'state':
            state  = fan_parse(p['value'])
            logtxt = '[MQTT publish|fan] {}'.format(state)
            mqttc.publish("kocom/livingroom/fan/state", json.dumps(state))

        elif p['src'] == 'gas':
            state  = {'state': p['cmd']}
            logtxt = '[MQTT publish|gas] {}'.format(state)
            mqttc.publish("kocom/livingroom/gas/state", json.dumps(state))

    elif p['type'] == 'send' and p['dest'] == 'elevator':
        floor      = int(p['value'][2:4], 16)
        rs485_floor= int(config.get('Elevator', 'rs485_floor', fallback=0))
        if rs485_floor != 0:
            state = {'floor': floor}
            if rs485_floor == floor:
                state['state'] = 'off'
        else:
            state = {'state': 'off'}
        logtxt = '[MQTT publish|elevator] {}'.format(state)
        mqttc.publish("kocom/myhome/elevator/state", json.dumps(state))

    if logtxt and config.get('Log', 'show_mqtt_publish') == 'True':
        logging.info(logtxt)


# ═══════════════════════════════════════════════════════════════════
#  MQTT Discovery
# ═══════════════════════════════════════════════════════════════════

def discovery():
    dev_list = [x.strip() for x in config.get('Device', 'enabled').split(',')]
    for t in dev_list:
        dev = t.split('_')
        sub = dev[1] if len(dev) > 1 else ''
        logtxt = '[MQTT Discovery|{}] sub={}'.format(dev[0], sub)
        publish_discovery(dev[0], sub)
        if logtxt and config.get('Log', 'show_mqtt_discovery') == 'True':
            logging.info(logtxt)
    publish_discovery('query')


def publish_discovery(dev, sub=''):
    _device_info = {
        'name': '코콤 스마트 월패드',
        'ids':  'kocom_smart_wallpad',
        'mf':   'KOCOM',
        'mdl':  '스마트 월패드',
        'sw':   SW_VERSION
    }

    if dev == 'fan':
        topic   = 'homeassistant/fan/kocom_wallpad_fan/config'
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
            'pl_on': 'on', 'pl_off': 'off', 'qos': 0,
            'uniq_id': 'kocom_wallpad_{}'.format(dev),
            'device': _device_info
        }
        mqttc.publish(topic, json.dumps(payload))

    elif dev == 'air':
        air_attr = {
            'pm10': ['molecule', 'µg/m³'], 'pm25': ['molecule', 'µg/m³'],
            'co2': ['molecule-co2', 'ppm'], 'tvocs': ['molecule', 'ppb'],
            'temperature': ['thermometer', '°C'],
            'humidity': ['water-percent', '%'],
            'score': ['periodic-table', '%']
        }
        for key, (icon, unit) in air_attr.items():
            topic   = 'homeassistant/sensor/kocom_wallpad_air_{}/config'.format(key)
            payload = {
                'name': 'kocom_air_{}'.format(key),
                'stat_t': 'kocom/livingroom/air/state',
                'val_tpl': '{{{{ value_json.{} }}}}'.format(key),
                'qos': 0, 'uniq_id': 'kocom_air_{}'.format(key),
                'icon': 'mdi:{}'.format(icon), 'unit_of_meas': unit,
                'device': _device_info
            }
            mqttc.publish(topic, json.dumps(payload), retain=True)

    elif dev == 'gas':
        topic   = 'homeassistant/switch/kocom_wallpad_gas/config'
        payload = {
            'name': 'Kocom Wallpad Gas',
            'cmd_t': 'kocom/livingroom/gas/command',
            'stat_t': 'kocom/livingroom/gas/state',
            'val_tpl': '{{ value_json.state }}',
            'pl_on': 'on', 'pl_off': 'off',
            'ic': 'mdi:gas-cylinder', 'qos': 0,
            'uniq_id': 'kocom_wallpad_{}'.format(dev),
            'device': _device_info
        }
        mqttc.publish(topic, json.dumps(payload))

    elif dev == 'elevator':
        topic   = 'homeassistant/switch/kocom_wallpad_elevator/config'
        payload = {
            'name': 'Kocom Wallpad Elevator',
            'cmd_t': 'kocom/myhome/elevator/command',
            'stat_t': 'kocom/myhome/elevator/state',
            'val_tpl': '{{ value_json.state }}',
            'pl_on': 'on', 'pl_off': 'off',
            'ic': 'mdi:elevator', 'qos': 0,
            'uniq_id': 'kocom_wallpad_{}'.format(dev),
            'device': _device_info
        }
        mqttc.publish(topic, json.dumps(payload))

    elif dev == 'light':
        for num in range(1, int(config.get('User', 'light_count')) + 1):
            topic   = 'homeassistant/light/kocom_{}_light{}/config'.format(sub, num)
            payload = {
                'name': 'Kocom {} Light{}'.format(sub, num),
                'cmd_t': 'kocom/{}/light/{}/command'.format(sub, num),
                'stat_t': 'kocom/{}/light/state'.format(sub),
                'stat_val_tpl': '{{{{ value_json.light_{} }}}}'.format(num),
                'pl_on': 'on', 'pl_off': 'off', 'qos': 0,
                'uniq_id': 'kocom_{}_{}{}'.format(sub, dev, num),
                'device': _device_info
            }
            mqttc.publish(topic, json.dumps(payload))

    elif dev == 'thermo':
        num     = int(room_h_dic.get(sub))
        topic   = 'homeassistant/climate/kocom_{}_thermostat/config'.format(sub)
        payload = {
            'name': 'Kocom {} Thermostat'.format(sub),
            'mode_cmd_t':  'kocom/room/thermo/{}/heat_mode/command'.format(num),
            'mode_stat_t': 'kocom/room/thermo/{}/state'.format(num),
            'mode_stat_tpl': '{{ value_json.heat_mode }}',
            'temp_cmd_t':  'kocom/room/thermo/{}/set_temp/command'.format(num),
            'temp_stat_t': 'kocom/room/thermo/{}/state'.format(num),
            'temp_stat_tpl': '{{ value_json.set_temp }}',
            'curr_temp_t': 'kocom/room/thermo/{}/state'.format(num),
            'curr_temp_tpl': '{{ value_json.cur_temp }}',
            'modes': ['off', 'heat'],
            'min_temp': 20, 'max_temp': 30, 'ret': 'false', 'qos': 0,
            'uniq_id': 'kocom_wallpad_{}{}'.format(dev, num),
            'device': _device_info
        }
        mqttc.publish(topic, json.dumps(payload))

    elif dev == 'ac':
        num     = int(room_h_dic.get(sub))
        topic   = 'homeassistant/climate/kocom_{}_ac/config'.format(num)
        payload = {
            'name': 'kocom_ac_{}'.format(num),
            'mode_cmd_t':  'kocom/room/ac/{}/ac_mode/command'.format(num),
            'mode_stat_t': 'kocom/room/ac/{}/state'.format(num),
            'mode_stat_tpl': '{{ value_json.state }}',
            'fan_mode_cmd_t':  'kocom/room/ac/{}/fan_mode/command'.format(num),
            'fan_mode_stat_t': 'kocom/room/ac/{}/state'.format(num),
            'fan_mode_stat_tpl': '{{ value_json.fan }}',
            'temp_cmd_t':  'kocom/room/ac/{}/set_temp/command'.format(num),
            'temp_stat_t': 'kocom/room/ac/{}/state'.format(num),
            'temp_stat_tpl': '{{ value_json.target }}',
            'curr_temp_t': 'kocom/room/ac/{}/state'.format(num),
            'curr_temp_tpl': '{{ value_json.temperature }}',
            'modes': ['off', 'cool', 'fan_only', 'dry', 'auto', 'heat'],
            'fan_modes': ['LOW', 'MEDIUM', 'HIGH'],
            'min_temp': 18, 'max_temp': 30,
            'uniq_id': 'kocom_ac_{}'.format(num),
            'device': {**_device_info, 'mdl': 'K스마트 월패드'}
        }
        mqttc.publish(topic, json.dumps(payload), retain=True)

    elif dev == 'query':
        topic   = 'homeassistant/button/kocom_wallpad_query/config'
        payload = {
            'name': 'Kocom Wallpad Query',
            'cmd_t': 'kocom/myhome/query/command', 'qos': 0,
            'uniq_id': 'kocom_wallpad_{}'.format(dev),
            'device': _device_info
        }
        mqttc.publish(topic, json.dumps(payload))

    if config.get('Log', 'show_mqtt_discovery') == 'True':
        logging.info('[MQTT Discovery|{}{}]'.format(dev, sub))


# ═══════════════════════════════════════════════════════════════════
#  폴링 & 시리얼 읽기 스레드
# ═══════════════════════════════════════════════════════════════════

def poll_state(enforce=False):
    global poll_timer
    poll_timer.cancel()

    dev_list        = [x.strip() for x in config.get('Device', 'enabled').split(',')]
    no_polling_list = ['wallpad', 'elevator']

    for thread_instance in thread_list:
        if not thread_instance.is_alive():
            logging.error('[THREAD] {} 중단됨. 재시작.'.format(thread_instance.name))
            thread_instance.start()

    for t in dev_list:
        dev = t.split('_')
        if dev[0] in no_polling_list:
            continue
        dev_id = device_h_dic.get(dev[0])
        sub_id = room_h_dic.get(dev[1]) if len(dev) > 1 else '00'
        if dev_id and sub_id:
            if query(dev_id + sub_id, publish=True, enforce=enforce)['flag'] is False:
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
            d     = rs485.read()
            hex_d = '{:02x}'.format(ord(d))
            buf  += hex_d

            if buf[:len(header_h)] != header_h[:len(buf)]:
                not_parsed_buf += buf
                buf = ''
                frame_start = not_parsed_buf.find(header_h, len(header_h))
                if frame_start < 0:
                    continue
                not_parsed_buf = not_parsed_buf[:frame_start]
                buf = not_parsed_buf[frame_start:]

            if not_parsed_buf:
                logging.info('[comm] not parsed: ' + not_parsed_buf)
                not_parsed_buf = ''

            if len(buf) == packet_size * 2:
                chksum_calc = chksum(buf[len(header_h):chksum_position*2])
                chksum_buf  = buf[chksum_position*2:chksum_position*2+2]
                if chksum_calc == chksum_buf and buf[-len(trailer_h):] == trailer_h:
                    if msg_q.full():
                        logging.error('msg_q 가득 참. listen_hexdata 스레드 오류 가능성.')
                    msg_q.put(buf)
                    buf = ''
                else:
                    logging.info('[comm] invalid packet {} expected chksum {}'.format(
                        buf, chksum_calc))
                    frame_start = buf.find(header_h, len(header_h))
                    if frame_start < 0:
                        not_parsed_buf += buf
                        buf = ''
                    else:
                        not_parsed_buf += buf[:frame_start]
                        buf = buf[frame_start:]

        except Exception as ex:
            logging.error("*** Read error.[{}]".format(ex))
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

        cache_data.insert(0, p_ret)
        if len(cache_data) > BUF_SIZE:
            del cache_data[-1]

        # ACK 매칭: 삼성 AC(접두사) + 기존 기기(완전 일치) 모두 지원
        if ack_matches(p_ret['data_h'], ack_data):
            ack_q.put(d)
            continue

        if not wait_target.empty():
            if p_ret['dest_h'] == wait_target.queue[0] and p_ret['type'] == 'ack':
                if ack_data:
                    logging.info("[ACK] ACK 없이 응답 패킷 수신. ACK OK로 처리.")
                    ack_q.put(d)
                    time.sleep(0.5)
                wait_q.put(p_ret)
                continue

        publish_status(p_ret)


# ═══════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        format='%(levelname)s[%(asctime)s]:%(message)s ',
        level=logging.DEBUG)

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    if config.get('RS485', 'type') == 'serial':
        import serial
        rs485 = RS485Wrapper(
            serial_port=config.get('RS485', 'serial_port', fallback=None))
    elif config.get('RS485', 'type') == 'socket':
        import socket
        rs485 = RS485Wrapper(
            socket_server=config.get('RS485', 'socket_server'),
            socket_port=int(config.get('RS485', 'socket_port')))
    else:
        logging.error('[CONFIG] RS485 type은 "serial" 또는 "socket"만 허용됩니다.')
        exit(1)

    if rs485.connect() is False:
        logging.error('[RS485] 연결 실패. 종료합니다.')
        exit(1)

    mqttc = init_mqttc()
    if mqttc is False:
        logging.error('[MQTT] 연결 실패. 종료합니다.')
        exit(1)

    msg_q      = queue.Queue(BUF_SIZE)
    ack_q      = queue.Queue(1)
    ack_data   = []
    wait_q     = queue.Queue(1)
    wait_target= queue.Queue(1)
    send_lock  = threading.Lock()
    poll_timer = threading.Timer(1, poll_state)
    cache_data = []

    # 삼성 AC 이중 패킷 처리용 큐
    ac_packet_queue = queue.Queue(50)

    thread_list = [
        threading.Thread(target=read_serial,      name='read_serial'),
        threading.Thread(target=listen_hexdata,   name='listen_hexdata'),
        # 삼성 AC 전용 스레드 (daemon: 메인 종료 시 자동 종료)
        threading.Thread(target=ac_packet_handler,name='ac_packet_handler',
                         daemon=True),
    ]
    for t in thread_list:
        t.start()

    poll_timer.start()
    discovery()
