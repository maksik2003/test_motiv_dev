import smtplib
from getpass import getpass         # Чтобы не палить пароль =)

MAIL_TO = 'MOTIVTELECOM'

def send_mail(
        msg: str = 'messgage to ' + MAIL_TO, 
        MAIL_TO: str = MAIL_TO
    ) -> None:
    """
    Param list:\n
    msg - text message\n
    MAIL_TO - when mail be send (ITEAMY or MOTIVTELECOM). 
    """
    MAIL_TO = MAIL_TO.upper()

    if MAIL_TO == 'ITEAMY':
        # Отправка на ITEAMY
        smtpObj = smtplib.SMTP('excas.corp.motiv', 25)                                      # Создаем подключение к SMTP почтового сервера
        smtpObj.starttls()                                                                  # Указываем тип соединения
        smtpObj.login('it_lunatic@motivtelecom.ru', getpass())                              # Авторизация
        send_to = ["lunatic@iteamy.pro"]                                                    # Список адресов, на которые будет отправлено письмо
        smtpObj.sendmail("it_lunatic@motivtelecom.ru", send_to, msg = msg)                  # Отправляем письмо, указав с какого адреса будет отправлено письмо, куда отправляем и само сообщение

    elif MAIL_TO == 'MOTIVTELECOM':
        # Отправка на MOTIVTELECOM
        smtpObj = smtplib.SMTP('smtp.iteamy.pro', 25)
        smtpObj.starttls()
        smtpObj.login('lunatic@iteamy.pro', getpass())
        send_to = ["it_lunatic@motivtelecom.ru"]
        smtpObj.sendmail("lunatic@iteamy.pro", send_to, msg = msg)

if __name__ == '__main__':
    
    message = [
        'Unique domains = ' + str( 1 ),
        'Processed elements = ' + str( 2 ),
        'Process time = ' + str( 3 )
    ]
    
    print('mail sended')
    send_mail(
        msg = '\n'.join(message)
    )