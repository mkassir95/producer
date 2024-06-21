#!/usr/bin/env python3
from kafka import KafkaProducer
import kafka
import time
from pymap3d import enu2geodetic
#import random

import rospy
from rospy_message_converter import json_message_converter
from sensor_msgs.msg import NavSatFix
from nav_msgs.msg import Odometry
from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus
from std_msgs.msg import String
import diagnostic_updater
import socket
no_path_following = False
try:
    from romea_path_msgs.msg import PathMatchingInfo2D
except ImportError:
    no_path_following = True


def safe_latitude(x):
    if x is None:
        return "NA"
    else:
        return str(f'{x :03.7f}')


def safe_longitude(x):
    if x is None:
        return "NA"
    else:
        return str(f'{x :03.7f}')


def safe_speed(x):
    if x is None:
        return "NA"
    else:
        return f'{x :0.2f}'


def safe_int(x):
    if x is None:
        return "NA"
    else:
        return str(x)


def safe_diagnostic(x):
    if x is None:
        return "NA"
    else:
        return str(x)


class Client:

    def __init__(self):
        self.code_counter = 0
        self.speed = 0.0
        self.point_id = None
        self.longitude = None
        self.latitude = None
        self.counter = 0
        self.diagGPSLevel = ""
        self.diagGPSLocText = ""
        self.diagGPSLocName = ""
        self.diagPathMatchingLevel = ""
        self.diagPathMatchingText = ""
        self.diagPathMatchingName = ""
        self.diagJoyLevel = ""
        self.diagJoyText = ""
        self.diagJoyName = ""
        self.state = ""
        self.diagList = ["/path_following/path_matching", "platoon/path_matching"]
        self.producer_error_msg = None

        self.anchor = tuple(rospy.get_param('~anchor'))
        self.id = rospy.get_param("~id", rospy.get_param("~ns", 0))
        self.host = rospy.get_param("~host")
        self.port = rospy.get_param("~port")
        self.algoList = rospy.get_param("~algoList")
        self.separator = rospy.get_param("separator", " ")
        self.publish_rate = rospy.get_param("~publish_rate", 10.)
        # Ajout topic: control_fsm/state pour récupérer état de la State Machine
        self.topic_gps = rospy.get_param("~topic_gps", "gps/fix")
        self.topic_path_follow_match = rospy.get_param("~topic_path_follow_match",
                                                       "path_following/path_matching_info")
        self.topic_diagnostics = rospy.get_param("~topic_diagnostics", "diagnostics")
        self.topic_fsm = rospy.get_param("~topic_fsm", "control_fsm/state")
        self.kafka_timeout = rospy.get_param('~kafka_timeout', 1.0)

        self.producer = KafkaProducer(
            bootstrap_servers=[str(self.host) + ":" + str(self.port)],
            # api_version=(0, 2, 0),
            acks=1,
            retries=2,
        )

        self.updater = diagnostic_updater.Updater()
        self.updater.setHardwareID('none')
        self.updater.add('status', self.update_self_diagnostic)
        # Common topics
        # rospy.Subscriber(self.topic_gps, NavSatFix, self.callback_gps)
        rospy.Subscriber('localisation/filtered_odom', Odometry, self.callback_odom)
        rospy.Subscriber(self.topic_diagnostics, DiagnosticArray, self.callback_diagnostic)
        rospy.Subscriber(self.topic_fsm, String, self.callback_fsm)
        rospy.Timer(rospy.Duration(1 / self.publish_rate), self.callback_timer)

        # Romea topics optional:
        if not no_path_following:
            rospy.Subscriber(self.topic_path_follow_match, PathMatchingInfo2D,
                             self.callback_path_matching)

    def callback_timer(self, event):

        # dateDeb = (str(int(round(time. time() * 1000))))
        #    message = '%s %d %03.7f %03.7f %f \r\n'%(event.current_real,self.id,self.latitude,self.longitude,self.speed)
        # Trame du message envoyé au websocket
        #priority = random.choice([0, 1])
        priority =0
        if self.speed < 0 or self.speed > 3.2:
            message_type = "Alert"
            #priority=1
            # val_speed=self.speed+10
        else:
            message_type = "Normal"
            # val_speed=self.speed

    #  if (self.id == 'follower'):
    #      self.id = 'ALPO_1'
    #  elif (self.id == 'leader') :
    #      self.id = 'ALPO_2'


        messagedd = ""
        messagedd += str(int(round(time.time() * 1000)))
        messagedd += self.separator
        messagedd += str(self.id)
        messagedd += self.separator
        messagedd += str(self.counter)
        messagedd += self.separator
        messagedd += str(safe_speed(self.speed))
        messagedd += self.separator
        messagedd += safe_latitude(self.latitude)
        messagedd += self.separator
        messagedd += safe_longitude(self.longitude)
        messagedd += self.separator
        messagedd += str(priority)
        messagedd += self.separator

        message = ""
        # Temps d'envoie de la trame
        #  message += str(event.current_real.to_nsec()/1000)
        message += str(int(round(time.time() * 1000)))
        message += self.separator
        # Identifiant du robot

        message += str(self.id)
        message += self.separator
        # Numéro de la trame correspondant au robot
        message += str(self.counter)
        message += self.separator
        # Latitude du robot
        message += safe_latitude(self.latitude)
        message += self.separator
        # Longitude du robot
        message += safe_longitude(self.longitude)
        message += self.separator
        # Vitesse du robot
        message += safe_speed(self.speed)
        #message += safe_speed(val_speed)
        message += self.separator
        # Numéro du point de la trajectoire
        message += safe_int(self.point_id)
        message += self.separator
        # Code de diagnostique du GPS
        message += safe_diagnostic(self.diagGPSLevel)
        message += self.separator
        # Code de diagnostique du chargement de la trajectoire
        message += safe_diagnostic(self.diagPathMatchingLevel)
        message += self.separator
        # Code de l'etat de la machine
        message += str(self.state)
        message += self.separator
        # Elément pour réaliser le Join pour Amina
        #message += message_type
        #message += self.separator
        # Elément marquant la fin de la trame
        #message += ''

        # Message diagnostic robot
        messageD = '{\"robot_id\":' + str(self.id) + ','
        messageD = messageD + self.diagJoyName + ':' + self.diagJoyText + ','
        messageD = messageD + 'LD_' + self.diagJoyName + ':' + str(self.diagJoyLevel) + ','
        #messageD = messageD + 'LD_' + '100:' + str(self.diagJoyLevel) + ','
        messageD = messageD + self.diagGPSLocName + ':' + self.diagGPSLocText + ','
        messageD = messageD + 'LD_' + self.diagGPSLocName + ':' + str(self.diagGPSLevel) + ','
        messageD = messageD + self.diagPathMatchingName + ':' + self.diagPathMatchingText + ','
        messageD = messageD + 'LD_' + self.diagPathMatchingName + ':' + str(
            self.diagPathMatchingLevel) + '}'
        host = '10.160.17.176'  # The server's hostname or IP address
        port = 5000        # The port used by the server

        try:
           with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
               robot_info_message = f"Robot ID: {self.id}, Longitude: {safe_longitude(self.longitude)}, Latitude: {safe_latitude(self.latitude)}, Speed: {safe_speed(self.speed)}"
                # Encode the message to bytes
               encoded_message = robot_info_message.encode('utf-8')
               s.connect((host, port))
               s.sendall(encoded_message)
               rospy.loginfo("Message sent to server.")
        except socket.error as e:
            rospy.logwarn("Socket error: %s", e)
        except Exception as e:
            rospy.logwarn("An error occurred: %s", e)

        #  rospy.loginfo(messageD)

        try:
            rospy.logwarn('----- try to send data...')
            rospy.logwarn(message)
            # rospy.loginfo(messageD)
            # test = self.producer.send("position_data", value=message.encode("utf-8"))
            msg_data = message.encode("utf-8")
            msg_alert = messagedd.encode("utf-8")
            if message_type == "Normal":
                self.producer.send("r2k_pos", value=msg_data).get(self.kafka_timeout)
            else:
                a_rob_future = self.producer.send("r2k_a_rob", value=msg_alert)
                pos_future = self.producer.send("r2k_pos", value=msg_data)

                # wait for all futures
                a_rob_future.get(self.kafka_timeout)
                pos_future.get(self.kafka_timeout)

            #self.producer.send("r2k_a_rob", value=messageD.encode("utf-8")).get(self.kafka_timeout)
            #else:
            #self.producer.send("r2k_pos", value=message.encode("utf-8")).get(self.kafka_timeout)
            # test2= self.producer.send("alert_robot", value=messageD.encode("utf-8"))
            # test2 = self.producer.send("r2k_a_rob", value=messageD.encode("utf-8"))
            # rospy.loginfo("----- data sent to kafka -----")
            self.producer_error_msg = None
            self.updater.update()
        except kafka.errors.KafkaTimeoutError as e:
            rospy.logwarn(f"Failed to send data to kafka: {e}")
            self.producer_error_msg = str(e)
            self.updater.force_update()

        # dateFin = str(int(round(time.time() * 1000)))
        #self.writeInFile(dateDeb+";"+dateFin, "/home/romea/ros_ws/dev/tiara_ws/src/ros2kafka/scripts/test_exec.csv")
        self.counter += 1
        #time.sleep(10)

    def callback_gps(self, msg):
        #   rospy.loginfo("gps")
        self.latitude = msg.latitude
        self.longitude = msg.longitude

    def callback_path_matching(self, msg):
        self.point_id = msg.matched_points[msg.tracked_matched_point_index].curve_index

    def callback_odom(self, msg):
        self.speed = msg.twist.twist.linear.x

        pos = msg.pose.pose.position
        coords = enu2geodetic(pos.x, pos.y, pos.z, *self.anchor)
        self.latitude = coords[0]
        self.longitude = coords[1]

    def callback_diagnostic(self, msg):
        #rospy.loginfo("Diagnostic et robot id:" + str(self.id)) #: "+ msg.status)
        json_str = json_message_converter.convert_ros_message_to_json(msg)
        # print(json_str)
        # diagStatus = msg.status
        # for diagDevices in diagStatus:
        #    rospy.loginfo("Diagnostic :"+ diagDevices.name)
        #    rospy.loginfo("Chaine de caractere:" + str(self.id)+"/path_following/path_matching")
        #    if diagDevices.name == "localisation_gps_plugin":
        #       rospy.loginfo("GPS")
        #       self.diagGPSLevel=diagDevices.level
        #       self.diagGPSLocText=diagDevices.message
        #       self.diagGPSLocName=diagDevices.name

        #    if (diagDevices.name.find(str(self.id)+"/path_following/path_matching") != -1):
        #       rospy.loginfo("Path Matching")
        #       self.diagPathMatchingLevel=diagDevices.level
        #       rospy.loginfo("Path Matching Level: "+ str(diagDevices.level))
        #       self.diagPathMatchingText=diagDevices.message
        #       rospy.loginfo("Path Matching Message: "+ str(diagDevices.message))
        #       self.diagPathMatchingName=diagDevices.name
        #       rospy.loginfo("Path Matching Name: "+ str(diagDevices.name))

        #    if diagDevices.name == "robot"+str(self.id)+"/joystick: Joystick Driver Status":
        #       self.diagJoyLevel=diagDevices.level
        #       self.diagJoyText=diagDevices.message
        #       self.diagJoyName="joystick"

    def callback_fsm(self, msg):
        self.state = msg.data
        self.state = self.state.replace(" ", "")

    def writeInFile(self, txt, fileName):
        with open(fileName, 'a') as openFile:
            print(txt, file=openFile)
            openFile.close()

    def update_self_diagnostic(self, stat):
        ''' Publish the state of the communication with the monitoring server '''
        if self.producer_error_msg:
            stat.summary(DiagnosticStatus.ERROR, self.producer_error_msg)
        else:
            stat.summary(DiagnosticStatus.OK, 'ok')
        return stat


if __name__ == '__main__':
    rospy.init_node('ros_client', anonymous=True)
    try:
        client = Client()
    except rospy.ROSInterruptException:
        pass
    rospy.spin()