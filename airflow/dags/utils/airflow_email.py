###############################################################################
# Name: airflow_email.py
# Description: This module contains class AirflowEmailSender to allow Airflow
#              DAG to send an alert upon completion or failure as a callback
#              function.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/weather_analysis
###############################################################################
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate


class AirflowEmailSender():
    """
    This class contains methods to send emails via SMTP on Airflow DAG script.
    
    The class relies on the local host hosted via SMTP debugger server on locally
    by running the command: $python -m smtpd -c DebuggingServer -n localhost:1025
    
    The class methods integrates as the Airflow DAG callback functions to
    send emails with DAG information.
    """

    def __init__(self, send_from, send_to):
        """
        Parameters
        ----------
        send_from: str
            Sender email address
        send_to: str
            Receiver email address
        """
        self.subject = ""
        self.text = ""
        self.send_from = send_from
        self.send_to = send_to
        self.smtp_host = "host.docker.internal"
        self.smtp_port = 1025

    def __build_msg(self):
        msg = MIMEMultipart("alternative")
        msg['From'] = self.send_from
        msg['To'] = self.send_to
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = self.subject
        msg.attach(MIMEText(self.text, "html"))

        return msg
    
    def send(self):
        with smtplib.SMTP(self.smtp_host, self.smtp_port) as conn:
            msg = self.__build_msg()
            conn.sendmail(self.send_from, self.send_to, msg.as_string())

    def dag_failure_alert(self, context):
        """
        This function sends out an alert email upon failure of the Airflow DAG.
        
        Parameters
        ----------
        context: dict
            Dictionary passed from the Airflow DAG containing execution information.
        """
        dag_id = context["task_instance"].dag_id
        self.subject = f"JOB FAILURE - {dag_id}"
        self.text = "Please investigate the failure."
        self.send()

    def dag_complete_alert(self, context):
        """
        This function sends out an alert email upon completion of the Airflow DAG.

        Parameters
        ----------
        context: dict
            Dictionary passed from the Airflow DAG containing execution information.
        """
        dag_id = context["task_instance"].dag_id
        self.subject = f"JOB Complete - {dag_id}"
        self.text = "{dag_id} has completed successfully."
        self.send()