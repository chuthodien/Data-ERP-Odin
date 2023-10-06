import smtplib

def send_email_after_dbt_test(sender_email, password, receiver_email):
    FROM = sender_email
    TO = receiver_email if isinstance(receiver_email, list) else [receiver_email]
    SUBJECT = 'Notice of data synchronization'
    TEXT = 'Successfully synchronized and run dbt_test '
    # Prepare actual message
    message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
    # Send email 
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.ehlo()
    server.starttls()
    server.login(sender_email, password)
    server.sendmail(FROM, TO, message)
    server.close()
