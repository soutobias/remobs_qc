def send_alert_mail(lat_now, lon_now, time_now, distance, plot_file):

    import os
    import sys
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.image import MIMEImage
    from email.mime.base import MIMEBase
    from email import encoders
    import smtplib
    import pandas as pd

    home = os.environ['HOME']
    sys.path.append(home)

    from user_config import EMAIL_FROM, ALERT_EMAIL_TO, ALERT_POSITION_SUBJECT, \
        ALERT_POSITION_CONTENT, remo_mail, remo_password

    ALERT_POSITION_SUBJECT = ALERT_POSITION_SUBJECT.format(buoy='BMO BR SANTOS')

    msg = MIMEMultipart('alternative')
    msg['Subject'] = ALERT_POSITION_SUBJECT
    msg['From'] = EMAIL_FROM

    msg['To'] = ", ".join(ALERT_EMAIL_TO)

    CONTENT = ALERT_POSITION_CONTENT.format(buoy='BMO BR SANTOS',
                                            date_time=str(time_now) + ' Z',
                                            last_lon= str(lat_now) + " °S",
                                            last_lat= str(lon_now) + " °W",
                                            distance_meters=str(distance) + ' metros')

    msg.attach(MIMEText(CONTENT))

    IMAGES = [plot_file]
    for file in IMAGES:
        try:
            with open(file, 'rb') as fp:
                img = MIMEImage(fp.read())
            img.add_header('Content-Disposition', 'attachment', filename="última posição bmo br")
            msg.attach(img)
        finally:

            server = smtplib.SMTP('smtp.gmail.com', 587)

            server.starttls()
            print("Login on Email...")
            server.login(remo_mail, remo_password)
            print("Sending email...")
            server.sendmail(remo_mail, ALERT_EMAIL_TO, msg.as_string())
            print("Done! Email sent!")
            server.quit()
            print("Quit Server and Email.\n Script Finished!")