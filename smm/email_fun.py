from __future__ import print_function
from googleapiclient.discovery import build
from apiclient import errors
from httplib2 import Http
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import mimetypes
import base64
from google.oauth2 import service_account
import os
import pickle


def create_message(sender, to, subject, message_text):
  """Create a message for an email.

  Args:
    sender: Email address of the sender.
    to: Email address of the receiver.
    subject: The subject of the email message.
    message_text: The text of the email message.

  Returns:
    An object containing a base64url encoded email object.
  """
  message = MIMEText(message_text)
  message['to'] = to
  message['from'] = sender
  message['subject'] = subject

  return {'raw': (base64.urlsafe_b64encode(message.as_bytes())).decode()}

def send_message(service, user_id, message):
  """Send an email message.

  Args:
    service: Authorized Gmail API service instance.
    user_id: User's email address. The special value "me"
    can be used to indicate the authenticated user.
    message: Message to be sent.

  Returns:
    Sent Message.
  """
  try:
    message = (service.users().messages().send(userId=user_id, body=message)
               .execute())
    print('Message Id: %s' % message['id'])
    return message
  except errors.HttpError as error:
    print('An error occurred: %s' % error)


def service_account_login():
  #SCOPES = ['https://www.googleapis.com/auth/gmail.send']
  #SERVICE_ACCOUNT_FILE = 'service-key.json'

  if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        credentials = pickle.load(token)


 # credentials = service_account.Credentials.from_service_account_file(
  #        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
  #delegated_credentials = credentials.with_subject(EMAIL_FROM)
  service = build('gmail', 'v1', credentials=credentials)

  return service


def create_message_with_attachment(
    sender, to, subject, message_text, list_files):
  """Create a message for an email.

  Args:
    sender: Email address of the sender.
    to: Email address of the receiver.
    subject: The subject of the email message.
    message_text: The text of the email message.
    file: The path to the file to be attached.

  Returns:
    An object containing a base64url encoded email object.
  """
  message = MIMEMultipart()
  message['to'] = to
  message['from'] = sender
  message['subject'] = subject

  msg = MIMEText(message_text)
  message.attach(msg)

  for file in list_files:
    

    content_type, encoding = mimetypes.guess_type(file)

    if content_type is None or encoding is not None:
      content_type = 'application/octet-stream'
    main_type, sub_type = content_type.split('/', 1)
    if main_type == 'text':
      fp = open(file, 'rb')
      msg = MIMEText(fp.read(), _subtype=sub_type, _charset = 'utf-8')
      fp.close()
    # elif main_type == 'image':
    #   fp = open(file, 'rb')
    #   msg = MIMEImage(fp.read(), _subtype=sub_type)
    #   fp.close()
    # elif main_type == 'audio':
    #   fp = open(file, 'rb')
    #   msg = MIMEAudio(fp.read(), _subtype=sub_type)
    #   fp.close()
    # else:
    #   fp = open(file, 'rb')
    #   msg = MIMEBase(main_type, sub_type)
    #   msg.set_payload(fp.read())
    #   fp.close()


    filename = os.path.basename(file)
    msg.add_header('Content-Disposition', 'attachment', filename=filename)
    message.attach(msg)


  return {'raw': (base64.urlsafe_b64encode(message.as_bytes())).decode()}

