### Использование Airflow для решения аналитических задач.
Будем использовать следующие данные из таблицы vgsales.csv о продажах игр в разных регионах.

#### Задания:
Сначала определим год, за какой будем смотреть данные. Сделать это можно так:     в питоне выполнить 1994 + hash(f‘{login}') % 23,  где {login} - ваш логин

Требуется составить DAG из нескольких тасок, в результате которого нужно будет найти ответы на следующие вопросы:
- Какая игра была самой продаваемой в этом году во всем мире?
- Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
- На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
- У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
- Сколько игр продались лучше в Европе, чем в Японии?

Финальный таск пишет в лог ответ на каждый вопрос. 
В DAG будет 7 тасков. По одному на каждый вопрос, таск с загрузкой данных и финальный таск который собирает все ответы. Настройка отправки сообщений в телеграмм по окончанию работы DAG
