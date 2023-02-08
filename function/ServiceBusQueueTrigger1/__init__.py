import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database
    conn = psycopg2.connect(
        host="techconf-server.postgres.database.azure.com",
        dbname="techconfdb",
        user="alshayie@techconf-server",
        password="Password1234")

    # create a cursor
    cur = conn.cursor()

    try:
        # TODO: Get notification message and subject from database using the notification_id
        sql_notify='SELECT message, subject FROM notification WHERE id= %s'
        args_notify=(notification_id) #(notification_id,)
        cur.execute(sql_notify,args_notify)
        msg_subj=cur.fetchall()
        for row in msg_subj:

            message=row[0]
            subject=row[1]

        # TODO: Get attendees email and name
        sql_attend='SELECT email, first_name FROM attendee'
        cur.execute(sql_attend)
        sql_attendees = cur.fetchall()

        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in sql_attendees:

            email=attendee[0]            
            first_name=attendee[1]

            sg = SendGridAPIClient(api_key=os.environ.get('SENDGRID_API_KEY'))

            from_email = Email("test@example.com")
            to_email = To({email}),
            personal_subject = subject,
            content = Content("text/plain", message)

            mail=Mail(from_email,to_email,personal_subject,content)
            sg.send(mail)
        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        attendees_total= 'Notified {} attendees'.format(len(sql_attendees))
        notify_update="UPDATE notification SET status= %s WHERE id= %s"
        notify_args=(attendees_total,notification_id)
        cur.execute(notify_update,notify_args)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        conn.close()