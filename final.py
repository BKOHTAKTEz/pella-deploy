import telebot
from telebot import types
import sqlite3
import random
import os
from dotenv import load_dotenv
import threading
import time

# Загружаем токен из .env файла
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')

if not BOT_TOKEN:
    print("❌ ОШИБКА: Токен бота не найден!")
    print("Создайте файл .env и добавьте в него:")
    print("BOT_TOKEN=ваш_токен_бота")
    exit(1)

bot = telebot.TeleBot(BOT_TOKEN)

# ID администратора для отправки отчётов
ADMIN_ID = 8273209192

# Хранилище для активных викторин
active_quizzes = {}
db_lock = threading.Lock()


def log_action(user_id, username, action, status="успешно"):
    """Отправляет отчёт о действии пользователя администратору"""
    try:
        text = (f"📊 <b>Отчёт о действии</b>\n"
                f"👤 <b>Пользователь:</b> {username or 'Аноним'} (ID: {user_id})\n"
                f"🔹 <b>Действие:</b> {action}\n"
                f"✅ <b>Статус:</b> {status}")
        bot.send_message(ADMIN_ID, text, parse_mode='HTML')
    except Exception as e:
        print(f"Не удалось отправить отчёт: {e}")


class MusicDatabase:
    """База данных музыкального бота"""

    def __init__(self):
        self.db_file = 'music_bot.db'
        self.init_database()

    def get_connection(self):
        """Создание соединения с БД"""
        conn = sqlite3.connect(self.db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def init_database(self):
        """Инициализация базы данных"""
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Таблица артистов
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS artists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                full_name TEXT,
                birth_date TEXT,
                death_date TEXT,
                country TEXT NOT NULL,
                city TEXT,
                years TEXT NOT NULL,
                genre TEXT NOT NULL,
                subgenre TEXT,
                members TEXT,
                formation_year INTEGER,
                disband_year INTEGER,
                labels TEXT,
                website TEXT,
                hits TEXT NOT NULL,
                album_count INTEGER,
                top_song TEXT,
                album_sales TEXT,
                facts TEXT NOT NULL,
                influence_rating INTEGER DEFAULT 5,
                spotify_listeners INTEGER DEFAULT 0,
                youtube_subscribers INTEGER DEFAULT 0,
                grammy_awards INTEGER DEFAULT 0,
                signature_style TEXT,
                similar_artists TEXT,
                is_solo_artist BOOLEAN DEFAULT 0,
                career_start_year INTEGER
            )
            ''')

            # Таблица пользователей
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                searches_count INTEGER DEFAULT 0,
                quizzes_played INTEGER DEFAULT 0,
                quiz_points INTEGER DEFAULT 0,
                best_quiz_score INTEGER DEFAULT 0,
                favorites_count INTEGER DEFAULT 0
            )
            ''')

            # Таблица поиска
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                query TEXT NOT NULL,
                artist_found BOOLEAN DEFAULT 0,
                artist_id INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            # Таблица результатов викторины
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS quiz_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                quiz_type TEXT NOT NULL,
                score INTEGER DEFAULT 0,
                questions_played INTEGER DEFAULT 0,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            # Таблица избранного
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_favorites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                artist_id INTEGER NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, artist_id)
            )
            ''')

            conn.commit()

            # Проверяем, есть ли данные
            cursor.execute('SELECT COUNT(*) FROM artists')
            count = cursor.fetchone()[0]

            if count == 0:
                self.populate_database(cursor)

            conn.commit()
            conn.close()
            print("✅ База данных инициализирована")

    def populate_database(self, cursor):
        """Заполнение базы данных артистами"""
        artists = [
            # Rock Legends - Группы
            ("AC/DC", "AC/DC", None, None, "Австралия", "Сидней", "1973-настоящее время", 
             "Rock Legends", "Hard Rock", "Ангус Янг, Брайан Джонсон", 1973, None,
             "Columbia, Atlantic", "https://acdc.com", 
             "Back In Black, Highway to Hell, Thunderstruck, You Shook Me All Night Long", 18,
             "Back In Black", "200 млн+ копий", 
             "Знамениты школьной формой и энергичными выступлениями. Группа названа в честь аббревиатуры переменного/постоянного тока.", 
             9, 52000000, 8800000, 0, "Энергичный хард-рок", "Queen, Led Zeppelin, Guns N' Roses", 0, None),
            
            ("Queen", "Queen", None, None, "Великобритания", "Лондон", "1970-настоящее время",
             "Rock Legends", "Arena Rock", "Фредди Меркьюри, Брайан Мэй, Роджер Тейлор, Джон Дикон", 1970, None,
             "EMI, Hollywood", "https://queenonline.com", 
             "Bohemian Rhapsody, We Will Rock You, We Are The Champions, Another One Bites the Dust", 15,
             "Bohemian Rhapsody", "300 млн+ копий",
             "Фредди Меркьюри мог охватить 4 октавы. Группа стала первой, чей клип показали на MTV.",
             10, 78000000, 24000000, 2, "Опера-рок", "The Beatles, Led Zeppelin, David Bowie", 0, None),
            
            ("The Beatles", "The Beatles", None, None, "Великобритания", "Ливерпуль", "1960-1970",
             "Rock Legends", "Beat Music", "Джон Леннон, Пол Маккартни, Джордж Харрисон, Ринго Старр", 1960, 1970,
             "Parlophone, Capitol", "https://thebeatles.com", 
             "Yesterday, Hey Jude, Let It Be, Come Together", 13,
             "Hey Jude", "600 млн+ копий",
             "Самая влиятельная рок-группа всех времен. Первые, кто использовал студийные эффекты в поп-музыке.",
             10, 35000000, 28000000, 7, "Бит, поп-рок", "The Rolling Stones, The Who, Beach Boys", 0, None),
            
            ("Led Zeppelin", "Led Zeppelin", None, None, "Великобритания", "Лондон", "1968-1980",
             "Rock Legends", "Hard Rock", "Джимми Пейдж, Роберт Плант, Джон Пол Джонс, Джон Бонэм", 1968, 1980,
             "Atlantic", "https://ledzeppelin.com", 
             "Stairway to Heaven, Kashmir, Whole Lotta Love, Black Dog", 9,
             "Stairway to Heaven", "300 млн+ копий",
             "Родоначальники хэви-метала. Джон Бонэм играл на барабанах без перчаток.",
             10, 41000000, 12000000, 1, "Блюз-рок, фолк-рок", "AC/DC, Deep Purple, Black Sabbath", 0, None),
            
            # Pop Icons - Сольные артисты
            ("Michael Jackson", "Michael Joseph Jackson", "1958-08-29", "2009-06-25", "США", "Гэри", "1964-2009",
             "Pop Icons", "Pop/R&B", "Майкл Джексон", None, None,
             "Motown, Epic", "https://michaeljackson.com", 
             "Billie Jean, Thriller, Beat It, Smooth Criminal", 10,
             "Billie Jean", "400 млн+ копий",
             "Король поп-музыки. Первый черный артист, чье видео показали на MTV. Обладатель 13 рекордов Гиннесса.",
             10, 55000000, 51000000, 13, "Поп, соул, фанк", "Prince, Madonna, Janet Jackson", 1, 1964),
            
            ("Madonna", "Madonna Louise Ciccone", "1958-08-16", None, "США", "Бей-Сити", "1979-настоящее время",
             "Pop Icons", "Pop/Dance", "Мадонна", None, None,
             "Sire, Warner", "https://madonna.com", 
             "Like a Virgin, Material Girl, Vogue, Hung Up", 14,
             "Like a Prayer", "300 млн+ копий",
             "Королева поп-музыки. Самая продаваемая женщина-артист в истории. Известна постоянными образами.",
             9, 42000000, 18000000, 7, "Поп, дэнс, электронная музыка", "Michael Jackson, Prince, Cher", 1, 1979),
            
            ("Taylor Swift", "Taylor Alison Swift", "1989-12-13", None, "США", "Рединг", "2006-настоящее время",
             "Pop Icons", "Country/Pop", "Тейлор Свифт", None, None,
             "Big Machine, Republic", "https://taylorswift.com", 
             "Love Story, Shake It Off, Blank Space, Anti-Hero", 10,
             "Shake It Off", "200 млн+ копий",
             "Перезаписала свои альбомы из-за споров о правах. Самая награждаемая артистка 2020-х годов.",
             9, 96000000, 68000000, 12, "Поп, кантри, синт-поп", "Katy Perry, Ariana Grande, Ed Sheeran", 1, 2006),
            
            # Metal - Группы
            ("Metallica", "Metallica", None, None, "США", "Лос-Анджелес", "1981-настоящее время",
             "Metal", "Thrash Metal", "Джеймс Хэтфилд, Ларс Ульрих, Кирк Хэмметт, Роберт Трухильо", 1981, None,
             "Megaforce, Elektra", "https://metallica.com", 
             "Enter Sandman, Nothing Else Matters, Master of Puppets, One", 11,
             "Enter Sandman", "125 млн+ копий",
             "Одна из 'большой четверки' трэш-метала. Выпустили альбом с симфоническим оркестром.",
             9, 65000000, 21000000, 9, "Трэш-метал", "Slayer, Megadeth, Anthrax", 0, None),
            
            ("Iron Maiden", "Iron Maiden", None, None, "Великобритания", "Лондон", "1975-настоящее время",
             "Metal", "Heavy Metal", "Брюс Дикинсон, Стив Харрис, Дэйв Мюррей", 1975, None,
             "EMI, Sanctuary", "https://ironmaiden.com", 
             "The Trooper, Fear of the Dark, Run to the Hills, The Number of the Beast", 17,
             "The Trooper", "100 млн+ копий",
             "Известны талисманом Эдди, который появляется на всех обложках. Группа владеет собственным самолетом.",
             8, 29000000, 5600000, 0, "Хэви-метал", "Judas Priest, Black Sabbath, Dio", 0, None),
            
            # Hip-Hop - Сольные артисты
            ("Eminem", "Marshall Bruce Mathers III", "1972-10-17", None, "США", "Сент-Джозеф", "1988-настоящее время",
             "Hip-Hop", "Hip-Hop/Rap", "Эминем", None, None,
             "Web, Aftermath", "https://eminem.com", 
             "Lose Yourself, Without Me, Love The Way You Lie, Rap God", 11,
             "Lose Yourself", "220 млн+ копий",
             "Первый белый рэпер, получивший премию 'Оскар'. Самый продаваемый рэпер 2000-х годов.",
             9, 85000000, 55000000, 15, "Хардкор-рэп", "Dr. Dre, 50 Cent, Kendrick Lamar", 1, 1988),
            
            ("Tupac Shakur", "Tupac Amaru Shakur", "1971-06-16", "1996-09-13", "США", "Нью-Йорк", "1988-1996",
             "Hip-Hop", "West Coast Hip-Hop", "Тупак Шакур", None, None,
             "Interscope, Death Row", "https://2pac.com", 
             "California Love, Changes, Dear Mama, Hit 'Em Up", 11,
             "California Love", "75 млн+ копий",
             "Один из самых влиятельных рэперов всех времен. Активный общественный деятель.",
             9, 45000000, 21000000, 0, "Хип-хоп, гангста-рэп", "The Notorious B.I.G., Snoop Dogg, Nas", 1, 1988),
            
            # EDM - Сольные артисты и группы
            ("David Guetta", "Pierre David Guetta", "1967-11-07", None, "Франция", "Париж", "1984-настоящее время",
             "EDM", "House/EDM", "Дэвид Гетта", None, None,
             "Virgin, Parlophone", "https://davidguetta.com", 
             "Titanium, When Love Takes Over, Memories, Without You", 7,
             "Titanium", "50 млн+ копий",
             "Пионер коммерческого EDM в поп-музыке. Один из самых высокооплачиваемых DJ в мире.",
             8, 55000000, 43000000, 2, "EDM, хаус", "Calvin Harris, Avicii, Martin Garrix", 1, 1984),
            
            ("Daft Punk", "Daft Punk", None, None, "Франция", "Париж", "1993-2021",
             "EDM", "French House", "Тома Бангальтер, Ги-Мануэль де Омем-Kристо", 1993, 2021,
             "Virgin, Columbia", "https://daftpunk.com", 
             "Get Lucky, Around the World, One More Time, Harder Better Faster Stronger", 4,
             "Get Lucky", "20 млн+ копий",
             "Никогда не показывали лица, всегда выступали в шлемах. Вдохновили целое поколение электронной музыки.",
             9, 48000000, 24000000, 6, "Френч-хаус, диско", "Justice, deadmau5, Disclosure", 0, None),
            
            # R&B - Сольные артисты
            ("Beyoncé", "Beyoncé Giselle Knowles", "1981-09-04", None, "США", "Хьюстон", "1997-настоящее время",
             "R&B", "R&B/Pop", "Бейонсе", None, None,
             "Columbia, Parkwood", "https://beyonce.com", 
             "Single Ladies, Halo, Crazy in Love, Formation", 6,
             "Single Ladies", "200 млн+ копий",
             "Самая номинированная артистка в истории Grammy. Бывшая участница Destiny's Child.",
             9, 62000000, 36000000, 32, "R&B, поп, соул", "Rihanna, Alicia Keys, Mary J. Blige", 1, 1997),
            
            ("The Weeknd", "Abel Makkonen Tesfaye", "1990-02-16", None, "Канада", "Торонто", "2010-настоящее время",
             "R&B", "Alternative R&B", "The Weeknd", None, None,
             "XO, Republic", "https://theweeknd.com", 
             "Blinding Lights, Starboy, Save Your Tears, The Hills", 5,
             "Blinding Lights", "75 млн+ копий",
             "Держал рекорд самой популярной песни в Spotify. Известен своими таинственными образами.",
             8, 85000000, 68000000, 4, "R&B, поп", "Drake, Frank Ocean, Bruno Mars", 1, 2010),
            
            # Alternative - Группы
            ("Nirvana", "Nirvana", None, None, "США", "Абердин", "1987-1994",
             "Alternative", "Grunge", "Курт Кобейн, Крист Новоселич, Дэйв Грол", 1987, 1994,
             "Sub Pop, DGC", "https://nirvana.com", 
             "Smells Like Teen Spirit, Come As You Are, Lithium, Heart-Shaped Box", 3,
             "Smells Like Teen Spirit", "75 млн+ копий",
             "Группа возглавила движение гранж в начале 1990-х. Курт Кобейн стал голосом поколения.",
             9, 45000000, 18000000, 0, "Гранж", "Pearl Jam, Soundgarden, Alice in Chains", 0, None),
            
            ("Radiohead", "Radiohead", None, None, "Великобритания", "Абингдон", "1985-настоящее время",
             "Alternative", "Alternative Rock", "Том Йорк, Джонни Гринвуд, Колин Гринвуд", 1985, None,
             "XL, Ticker Tape", "https://radiohead.com", 
             "Creep, Karma Police, Paranoid Android, No Surprises", 9,
             "Creep", "30 млн+ копий",
             "Одна из самых инновационных рок-групп. Отказались от традиционной музыкальной индустрии.",
             8, 31000000, 12000000, 3, "Арт-рок, электронная музыка", "Pink Floyd, Muse, Arcade Fire", 0, None),
            
            # Reggae - Сольный артист
            ("Bob Marley", "Robert Nesta Marley", "1945-02-06", "1981-05-11", "Ямайка", "Найн-Майлс", "1963-1981",
             "Reggae", "Reggae", "Боб Марли", None, None,
             "Studio One, Island", "https://bobmarley.com", 
             "No Woman No Cry, One Love, Redemption Song, Buffalo Soldier", 13,
             "One Love", "75 млн+ копий",
             "Самый известный исполнитель регги в мире. Икона растафарианства и символ мира.",
             10, 45000000, 21000000, 0, "Рэгги, ска", "Peter Tosh, Jimmy Cliff, UB40", 1, 1963),
            
            # Punk - Группы
            ("Green Day", "Green Day", None, None, "США", "Беркли", "1987-настоящее время",
             "Punk", "Punk Rock", "Билли Джо Армстронг, Майк Дёрнт, Тре Кул", 1987, None,
             "Reprise, Warner", "https://greenday.com", 
             "Basket Case, American Idiot, Good Riddance, Boulevard of Broken Dreams", 13,
             "Basket Case", "75 млн+ копий",
             "Вернули интерес к панк-року в мейнстриме. Рок-опера 'American Idiot' стала культовой.",
             8, 52000000, 26000000, 5, "Поп-панк", "Blink-182, The Offspring, Sum 41", 0, None),
            
            # Indie - Группы
            ("Arctic Monkeys", "Arctic Monkeys", None, None, "Великобритания", "Шеффилд", "2002-настоящее время",
             "Indie", "Indie Rock", "Алекс Тёрнер, Джейми Кук, Ник О'Мэлли, Мэтт Хелдерс", 2002, None,
             "Domino, Warner", "https://arcticmonkeys.com", 
             "Do I Wanna Know?, I Bet You Look Good on the Dancefloor, 505, Fluorescent Adolescent", 7,
             "Do I Wanna Know?", "20 млн+ копий",
             "Британская инди-рок группа, быстро добившаяся мировой известности. Дебютный альбом стал самым продаваемым в истории Великобритании.",
             8, 55000000, 28000000, 2, "Инди-рок, пост-панк", "The Strokes, Franz Ferdinand, The Killers", 0, None),
            
            # Classical - Сольный композитор
            ("Wolfgang Amadeus Mozart", "Wolfgang Amadeus Mozart", "1756-01-27", "1791-12-05", "Австрия", "Зальцбург", "1761-1791",
             "Classical", "Classical", "Вольфганг Амадей Моцарт", None, None,
             "", "", 
             "Eine kleine Nachtmusik, Requiem, Symphony No. 40, The Marriage of Figaro", 600,
             "Eine kleine Nachtmusik", "Неизвестно",
             "Начал сочинять в 5 лет. Написал более 600 произведений за свою короткую жизнь.",
             10, 12000000, 4500000, 0, "Классицизм", "Beethoven, Bach, Haydn", 1, 1761),
            
            # Jazz - Сольный артист
            ("Louis Armstrong", "Louis Daniel Armstrong", "1901-08-04", "1971-07-06", "США", "Новый Орлеан", "1919-1971",
             "Jazz", "Traditional Jazz", "Луи Армстронг", None, None,
             "Columbia, Decca", "", 
             "What a Wonderful World, Hello Dolly, La Vie En Rose, When the Saints Go Marching In", 0,
             "What a Wonderful World", "Неизвестно",
             "Пионер скэт-вокала. Один из самых влиятельных джазовых музыкантов в истории.",
             10, 18000000, 2800000, 1, "Джаз, диксиленд", "Duke Ellington, Ella Fitzgerald, Miles Davis", 1, 1919),
            
            # K-Pop - Группы
            ("BTS", "Bangtan Sonyeondan", None, None, "Южная Корея", "Сеул", "2013-настоящее время",
             "K-Pop", "K-Pop/Hip-Hop", "RM, Джин, Шуга, J-Hope, Чимин, Ви, Чонгук", 2013, None,
             "Big Hit Music", "https://bts.ibighit.com", 
             "Dynamite, Butter, Boy With Luv, Spring Day", 7,
             "Dynamite", "40 млн+ копий",
             "Первый корейский артист, возглавивший Billboard Hot 100. Группа-рекордсмен по количеству наград.",
             9, 72000000, 84000000, 0, "K-Pop, хип-хоп", "Blackpink, EXO, Twice", 0, None),
            
            # Latin - Сольный артист
            ("Shakira", "Shakira Isabel Mebarak Ripoll", "1977-02-02", None, "Колумбия", "Барранкилья", "1990-настоящее время",
             "Latin", "Latin Pop", "Шакира", None, None,
             "Sony Latin, Epic", "https://shakira.com", 
             "Hips Don't Lie, Whenever Wherever, Waka Waka, La Tortura", 11,
             "Hips Don't Lie", "80 млн+ копий",
             "Самый просматриваемый латинский артист на YouTube. Известна уникальными движениями бедрами.",
             8, 65000000, 51000000, 3, "Латин-поп, рок", "Ricky Martin, Jennifer Lopez, Gloria Estefan", 1, 1990),
            
            # Country - Сольный артист
            ("Johnny Cash", "John R. Cash", "1932-02-26", "2003-09-12", "США", "Кингсленд", "1954-2003",
             "Country", "Country", "Джонни Кэш", None, None,
             "Sun, Columbia", "https://johnnycash.com", 
             "Ring of Fire, Hurt, I Walk the Line, Folsom Prison Blues", 96,
             "Ring of Fire", "90 млн+ копий",
             'Член "Зала славы кантри-музыки" и "Зала славы рок-н-ролла". Известен как "Человек в черном".',
             9, 28000000, 9500000, 0, "Кантри, рокабилли", "Willie Nelson, Bob Dylan, Elvis Presley", 1, 1954),
            
            # Современные популярные исполнители - Смешанные
            ("Drake", "Aubrey Drake Graham", "1986-10-24", None, "Канада", "Торонто", "2001-настоящее время",
             "Hip-Hop", "Pop Rap", "Дрейк", None, None,
             "Young Money, Cash Money, Republic", "https://drakerelated.com", 
             "God's Plan, Hotline Bling, One Dance, In My Feelings", 8,
             "God's Plan", "170 млн+ копий",
             "Самый прослушиваемый артист в мире на Spotify. Установил множество рекордов в Billboard Hot 100.",
             9, 110000000, 42000000, 5, "Поп-рэп, R&B", "The Weeknd, Travis Scott, Future", 1, 2001),
            
            ("Ariana Grande", "Ariana Grande-Butera", "1993-06-26", None, "США", "Бока-Ратон", "2008-настоящее время",
             "Pop Icons", "Pop/R&B", "Ариана Гранде", None, None,
             "Republic", "https://arianagrande.com", 
             "Thank U, Next, 7 Rings, Positions, Problem", 7,
             "Thank U, Next", "100 млн+ копий",
             "Обладательница самого высокого вокального диапазона среди поп-певиц. Бывшая актриса детских сериалов.",
             8, 88000000, 52000000, 2, "Поп, R&B", "Taylor Swift, Selena Gomez, Demi Lovato", 1, 2008),
            
            ("Ed Sheeran", "Edward Christopher Sheeran", "1991-02-17", None, "Великобритания", "Галифакс", "2004-настоящее время",
             "Pop Icons", "Pop/Folk", "Эд Ширан", None, None,
             "Asylum, Atlantic", "https://edsheeran.com", 
             "Shape of You, Perfect, Thinking Out Loud, Bad Habits", 7,
             "Shape of You", "150 млн+ копий",
             "Один из самых продаваемых музыкантов 2010-х. Известен тем, что выступает соло с loop-станцией.",
             8, 112000000, 52000000, 4, "Поп, фолк", "Taylor Swift, James Bay, Passenger", 1, 2004),
            
            ("Billie Eilish", "Billie Eilish Pirate Baird O'Connell", "2001-12-18", None, "США", "Лос-Анджелес", "2015-настоящее время",
             "Alternative", "Alt-Pop", "Билли Айлиш", None, None,
             "Darkroom, Interscope", "https://billieeilish.com", 
             "Bad Guy, Ocean Eyes, Happier Than Ever, Therefore I Am", 3,
             "Bad Guy", "50 млн+ копий",
             "Самая молодая артистка, выигравшая все четыре основные категории Grammy в один год.",
             8, 75000000, 48000000, 7, "Альтернативный поп, электроника", "Lorde, Melanie Martinez, FINNEAS", 1, 2015),
            
            ("Post Malone", "Austin Richard Post", "1995-07-04", None, "США", "Сиракузы", "2015-настоящее время",
             "Hip-Hop", "Pop Rap", "Пост Мэлоун", None, None,
             "Republic", "https://postmalone.com", 
             "Rockstar, Sunflower, Circles, Congratulations", 5,
             "Rockstar", "120 млн+ копий",
             "Известен уникальным стилем, сочетающим хип-хоп, поп и кантри. Многократный рекордсмен Billboard.",
             8, 82000000, 43000000, 0, "Поп-рэп, рок", "Drake, Travis Scott, Swae Lee", 1, 2015),
            
            ("Dua Lipa", "Dua Lipa", "1995-08-22", None, "Великобритания", "Лондон", "2014-настоящее время",
             "Pop Icons", "Dance-Pop", "Дуа Липа", None, None,
             "Warner", "https://dualipa.com", 
             "New Rules, Don't Start Now, Levitating, Physical", 3,
             "Don't Start Now", "80 млн+ копий",
             "Первый артист, чьи два клипа набрали по 1 миллиарду просмотров на YouTube.",
             8, 78000000, 38000000, 3, "Дэнс-поп, диско", "The Weeknd, Doja Cat, Miley Cyrus", 1, 2014),
            
            ("Bruno Mars", "Peter Gene Hernandez", "1985-10-08", None, "США", "Гонолулу", "2004-настоящее время",
             "Pop Icons", "Pop/R&B", "Бруно Марс", None, None,
             "Atlantic, Elektra", "https://brunomars.com", 
             "Uptown Funk, Just the Way You Are, Grenade, That's What I Like", 4,
             "Uptown Funk", "140 млн+ копий",
             "Один из самых успешных артистов 2010-х годов. Известен как 'король попа' за свой ретро-стиль.",
             8, 85000000, 33000000, 11, "Поп, R&B, фанк", "Anderson .Paak, Mark Ronson, Pharrell Williams", 1, 2004),
            
            ("Rihanna", "Robyn Rihanna Fenty", "1988-02-20", None, "Барбадос", "Сент-Майкл", "2005-настоящее время",
             "Pop Icons", "Pop/R&B", "Рианна", None, None,
             "Def Jam, Roc Nation", "https://rihanna.com", 
             "Umbrella, Diamonds, Work, We Found Love", 8,
             "Umbrella", "180 млн+ копий",
             "Одна из самых продаваемых артисток всех времен. Основательница успешного бренда Fenty Beauty.",
             9, 56000000, 54000000, 9, "Поп, R&B, дэнсхолл", "Beyoncé, Drake, Chris Brown", 1, 2005),
            
            ("Kanye West", "Kanye Omari West", "1977-06-08", None, "США", "Атланта", "1996-настоящее время",
             "Hip-Hop", "Hip-Hop", "Канье Уэст", None, None,
             "GOOD Music, Def Jam", "https://kanyewest.com", 
             "Stronger, Gold Digger, Heartless, Power", 10,
             "Stronger", "100 млн+ копий",
             "Один из самых влиятельных хип-хоп продюсеров. Основатель бренда Yeezy и музыкального лейбла GOOD Music.",
             9, 45000000, 18000000, 22, "Хип-хоп, экспериментальный", "Jay-Z, Kid Cudi, Travis Scott", 1, 1996),
            
            ("Harry Styles", "Harry Edward Styles", "1994-02-01", None, "Великобритания", "Холмс-Чапел", "2010-настоящее время",
             "Pop Icons", "Pop/Rock", "Гарри Стайлз", None, None,
             "Columbia, Erskine", "https://hstyles.co.uk", 
             "Watermelon Sugar, Sign of the Times, As It Was, Adore You", 3,
             "Watermelon Sugar", "70 млн+ копий",
             "Бывший участник группы One Direction. Первый мужчина-солист, появившийся в одиночку на обложке Vogue.",
             8, 72000000, 29000000, 2, "Поп-рок, софт-рок", "Niall Horan, Lewis Capaldi, Sam Fender", 1, 2010),
            
            ("Bad Bunny", "Benito Antonio Martínez Ocasio", "1994-03-10", None, "Пуэрто-Рико", "Вега-Баха", "2016-настоящее время",
             "Latin", "Reggaeton/Latin Trap", "Бэд Банни", None, None,
             "Rimas Entertainment", "https://badbunny.pr", 
             "Dákiti, Yonaguni, Callaita, Me Porto Bonito", 5,
             "Dákiti", "90 млн+ копий",
             "Самый прослушиваемый артист в мире на Spotify в 2020-2022 годах. Пионер латин-трэпа.",
             9, 95000000, 45000000, 4, "Реггетон, латин-трэп", "J Balvin, Ozuna, Rauw Alejandro", 1, 2016),
            
            ("Travis Scott", "Jacques Bermon Webster II", "1991-04-30", None, "США", "Хьюстон", "2008-настоящее время",
             "Hip-Hop", "Trap Rap", "Трэвис Скотт", None, None,
             "Grand Hustle, Epic, Cactus Jack", "https://travisscott.com", 
             "SICKO MODE, Goosebumps, Highest in the Room, Antidote", 4,
             "SICKO MODE", "110 млн+ копий",
             "Известен своими инновационными live-выступлениями и коллаборациями. Создатель фестиваля Astroworld.",
             8, 68000000, 35000000, 0, "Трэп, хип-хоп", "Kanye West, Kid Cudi, Playboi Carti", 1, 2008),
            
            ("Doja Cat", "Amala Ratna Zandile Dlamini", "1995-10-21", None, "США", "Лос-Анджелес", "2014-настоящее время",
             "Pop Icons", "Pop/Rap", "Доджа Кэт", None, None,
             "Kemosabe, RCA", "https://dojacat.com", 
             "Say So, Kiss Me More, Woman, Need to Know", 3,
             "Say So", "80 млн+ копий",
             "Известна вирусными танцевальными треками и активностью в социальных сетях. Победитель Grammy.",
             8, 74000000, 28000000, 1, "Поп, R&B, хип-хоп", "Megan Thee Stallion, Nicki Minaj, Ariana Grande", 1, 2014),
            
            ("Olivia Rodrigo", "Olivia Rodrigo", "2003-02-20", None, "США", "Темекьюла", "2015-настоящее время",
             "Pop Icons", "Pop/Punk", "Оливия Родриго", None, None,
             "Geffen, Interscope", "https://oliviarodrigo.com", 
             "drivers license, good 4 u, deja vu, vampire", 2,
             "drivers license", "60 млн+ копий",
             "Самый быстрый дебют в истории Spotify. Бывшая актриса Disney Channel.",
             8, 65000000, 22000000, 3, "Поп-панк, инди-поп", "Taylor Swift, Conan Gray, Gracie Abrams", 1, 2015),
            
            ("Kendrick Lamar", "Kendrick Lamar Duckworth", "1987-06-17", None, "США", "Комптон", "2003-настоящее время",
             "Hip-Hop", "Conscious Hip-Hop", "Кендрик Ламар", None, None,
             "Top Dawg, Aftermath, Interscope", "https://oklama.com", 
             "HUMBLE., DNA., Alright, Swimming Pools (Drank)", 5,
             "HUMBLE.", "85 млн+ копий",
             "Единственный неклассический музыкант, получивший Пулитцеровскую премию. Известен концептуальными альбомами.",
             9, 58000000, 25000000, 17, "Концептуальный хип-хоп", "J. Cole, Drake, Schoolboy Q", 1, 2003),
            
            ("SZA", "Solána Imani Rowe", "1989-11-08", None, "США", "Сент-Луис", "2012-настоящее время",
             "R&B", "Alternative R&B", "SZA", None, None,
             "Top Dawg, RCA", "https://szactrl.com", 
             "Kill Bill, Good Days, Love Galore, The Weekend", 2,
             "Kill Bill", "55 млн+ копий",
             "Известна своим уникальным вокалом и лирикой. Её дебютный альбом получил 5 номинаций на Grammy.",
             8, 62000000, 19000000, 1, "Альтернативный R&B, нео-соул", "Frank Ocean, Summer Walker, Jhené Aiko", 1, 2012),
            
            ("Miley Cyrus", "Miley Ray Cyrus", "1992-11-23", None, "США", "Нашвилл", "2001-настоящее время",
             "Pop Icons", "Pop/Rock", "Майли Сайрус", None, None,
             "Columbia, RCA", "https://mileycyrus.com", 
             "Wrecking Ball, Party in the USA, Flowers, Malibu", 8,
             "Flowers", "95 млн+ копий",
             "Дочь кантри-певца Билли Рэя Сайруса. Известна радикальными сменами имиджа на протяжении карьеры.",
             8, 72000000, 51000000, 0, "Поп-рок, кантри-поп", "Dolly Parton, Billy Ray Cyrus, Selena Gomez", 1, 2001),
            
            ("Lana Del Rey", "Elizabeth Woolridge Grant", "1985-06-21", None, "США", "Нью-Йорк", "2005-настоящее время",
             "Alternative", "Dream Pop", "Лана Дель Рей", None, None,
             "Interscope, Polydor", "https://lanadelrey.com", 
             "Video Games, Summertime Sadness, Young and Beautiful, Born to Die", 9,
             "Summertime Sadness", "70 млн+ копий",
             "Известна своим уникальным стилем, сочетающим ретро-эстетику и меланхоличную лирику.",
             8, 53000000, 27000000, 0, "Дрим-поп, барокко-поп", "Marina, Lorde, Florence + The Machine", 1, 2005),
            
            ("Coldplay", "Coldplay", None, None, "Великобритания", "Лондон", "1998-настоящее время",
             "Alternative", "Alternative Rock", "Крис Мартин, Джонни Баклэнд, Гай Берриман, Уилл Чемпион", 1998, None,
             "Parlophone, Capitol", "https://coldplay.com", 
             "Yellow, Viva La Vida, Paradise, The Scientist", 9,
             "Viva La Vida", "100 млн+ копий",
             "Одна из самых коммерчески успешных рок-групп в мире. Известны благотворительной деятельностью.",
             8, 68000000, 39000000, 7, "Альтернативный рок, поп-рок", "U2, Radiohead, Keane", 0, None),
            
            ("Imagine Dragons", "Imagine Dragons", None, None, "США", "Лас-Вегас", "2008-настоящее время",
             "Alternative", "Pop Rock", "Дэн Рейнольдс, Дэниел Платцман, Бен МакКи, Дэниел Уэйн Сермон", 2008, None,
             "Interscope, KIDinaKORNER", "https://imaginedragonsmusic.com", 
             "Radioactive, Demons, Believer, Thunder", 6,
             "Radioactive", "90 млн+ копий",
             "Дебютный сингл 'Radioactive' стал самым успешным рок-синглом в истории Billboard.",
             8, 74000000, 42000000, 1, "Поп-рок, электро-рок", "OneRepublic, X Ambassadors, The Script", 0, None),
            
            ("Maroon 5", "Maroon 5", None, None, "США", "Лос-Анджелес", "1994-настоящее время",
             "Pop Icons", "Pop/Rock", "Адам Левин, Джеймс Валентайн, Джесси Кармайкл, Мэтт Флинн, PJ Мортон", 1994, None,
             "A&M, Octone, Interscope", "https://maroon5.com", 
             "Moves Like Jagger, Girls Like You, Sugar, This Love", 7,
             "Moves Like Jagger", "120 млн+ копий",
             "Группа, начавшая как кара-фанк коллектив. Лидер Адам Левин - судья в шоу 'The Voice'.",
             8, 62000000, 35000000, 3, "Поп-рок, фанк-поп", "OneRepublic, The Script, Train", 0, None),
            
            ("Blackpink", "Blackpink", None, None, "Южная Корея", "Сеул", "2016-настоящее время",
             "K-Pop", "K-Pop", "Джису, Дженни, Розэ, Лиса", 2016, None,
             "YG Entertainment, Interscope", "https://blackpinkofficial.com", 
             "Ddu-Du Ddu-Du, Kill This Love, How You Like That, Pink Venom", 3,
             "Ddu-Du Ddu-Du", "60 млн+ копий",
             "Самый популярный к-поп гёрл-группа в мире. Первая к-поп группа на главной сцене Coachella.",
             9, 59000000, 92000000, 0, "K-Pop, EDM, хип-хоп", "BTS, Twice, (G)I-DLE", 0, None),
            
            ("One Direction", "One Direction", None, None, "Великобритания", "Лондон", "2010-2016",
             "Pop Icons", "Pop", "Гарри Стайлз, Найл Хоран, Лиам Пейн, Луи Томлинсон (ранее Зейн Малик)", 2010, 2016,
             "Syco, Columbia", "https://onedirectionmusic.com", 
             "What Makes You Beautiful, Story of My Life, Drag Me Down, Best Song Ever", 5,
             "What Makes You Beautiful", "150 млн+ копий",
             "Образованы на шоу 'The X Factor'. Стали самой успешной бойз-бэнд в истории после The Beatles.",
             8, 52000000, 42000000, 7, "Поп, тин-поп", "The Wanted, 5 Seconds of Summer, Why Don't We", 0, None),
            
            ("Lady Gaga", "Stefani Joanne Angelina Germanotta", "1986-03-28", None, "США", "Нью-Йорк", "2005-настоящее время",
             "Pop Icons", "Pop/Electro", "Леди Гага", None, None,
             "Streamline, Interscope, KonLive", "https://ladygaga.com", 
             "Bad Romance, Poker Face, Shallow, Born This Way", 7,
             "Bad Romance", "180 млн+ копий",
             "Известна экстравагантными костюмами и поддержкой ЛГБТ-сообщества. Обладательница 'Оскара'.",
             9, 70000000, 54000000, 13, "Электропоп, арт-поп", "Madonna, Katy Perry, Beyoncé", 1, 2005),
            
            ("Katy Perry", "Katheryn Elizabeth Hudson", "1984-10-25", None, "США", "Санта-Барбара", "2001-настоящее время",
             "Pop Icons", "Pop", "Кэти Перри", None, None,
             "Capitol", "https://katyperry.com", 
             "Firework, Roar, Dark Horse, California Gurls", 6,
             "Roar", "160 млн+ копий",
             "Первая артистка с пятью синглами №1 из одного альбома. Бывшая жена Рассела Брэнда.",
             8, 65000000, 45000000, 0, "Поп, электропоп", "Taylor Swift, Lady Gaga, P!nk", 1, 2001),
        ]

        cursor.executemany('''
        INSERT INTO artists (name, full_name, birth_date, death_date, country, city, years, 
                          genre, subgenre, members, formation_year, disband_year, labels, website, 
                          hits, album_count, top_song, album_sales, facts, influence_rating, 
                          spotify_listeners, youtube_subscribers, grammy_awards, signature_style, similar_artists, is_solo_artist, career_start_year)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', artists)

        print(f"✅ Добавлено {len(artists)} артистов в базу данных")

    # ---- Методы для работы с артистами ----
    def search_artist(self, name):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM artists WHERE LOWER(name) LIKE LOWER(?) OR LOWER(full_name) LIKE LOWER(?)',
                           (f'%{name}%', f'%{name}%'))
            result = cursor.fetchone()
            conn.close()
            return dict(result) if result else None

    def get_artist_by_id(self, artist_id):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM artists WHERE id = ?', (artist_id,))
            result = cursor.fetchone()
            conn.close()
            return dict(result) if result else None

    def get_artists_by_genre(self, genre):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT id, name FROM artists WHERE genre = ? ORDER BY influence_rating DESC', (genre,))
            results = cursor.fetchall()
            conn.close()
            return [dict(row) for row in results]

    def get_all_genres(self):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT genre FROM artists ORDER BY genre')
            results = cursor.fetchall()
            conn.close()
            return [row[0] for row in results]

    def get_random_artist(self):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM artists ORDER BY RANDOM() LIMIT 1')
            result = cursor.fetchone()
            conn.close()
            return dict(result) if result else None

    def get_random_artists(self, count=4, exclude_id=None):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            if exclude_id:
                cursor.execute('SELECT * FROM artists WHERE id != ? ORDER BY RANDOM() LIMIT ?', (exclude_id, count))
            else:
                cursor.execute('SELECT * FROM artists ORDER BY RANDOM() LIMIT ?', (count,))
            results = cursor.fetchall()
            conn.close()
            return [dict(row) for row in results]

    # ---- Методы для сравнения ----
    def compare_artists(self, artist1_id, artist2_id):
        artist1 = self.get_artist_by_id(artist1_id)
        artist2 = self.get_artist_by_id(artist2_id)
        if not artist1 or not artist2:
            return None
        comparison = {'artist1': artist1, 'artist2': artist2, 'similarities': [], 'differences': []}
        # жанры
        if artist1['genre'] == artist2['genre']:
            comparison['similarities'].append(f"Оба из жанра {artist1['genre']}")
        else:
            comparison['differences'].append(f"Жанры: {artist1['genre']} vs {artist2['genre']}")
        # страны
        if artist1['country'] == artist2['country']:
            comparison['similarities'].append(f"Оба из {artist1['country']}")
        else:
            comparison['differences'].append(f"Страны: {artist1['country']} vs {artist2['country']}")
        # влияние
        if abs(artist1['influence_rating'] - artist2['influence_rating']) <= 2:
            comparison['similarities'].append("Схожий уровень влияния")
        else:
            diff = abs(artist1['influence_rating'] - artist2['influence_rating'])
            comparison['differences'].append(f"Разница во влиянии: {diff} баллов")
        # Spotify
        if artist1['spotify_listeners'] and artist2['spotify_listeners']:
            ratio = artist1['spotify_listeners'] / artist2['spotify_listeners']
            if 0.8 < ratio < 1.2:
                comparison['similarities'].append("Схожая популярность на Spotify")
            else:
                name1 = artist1['name']
                name2 = artist2['name']
                if artist1['spotify_listeners'] > artist2['spotify_listeners']:
                    comparison['differences'].append(f"{name1} популярнее на Spotify")
                else:
                    comparison['differences'].append(f"{name2} популярнее на Spotify")
        return comparison

    # ---- Методы для викторины ----
    def get_quiz_question(self, question_type):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            if question_type == "genre":
                cursor.execute('SELECT id, name, genre FROM artists ORDER BY RANDOM() LIMIT 1')
                artist = cursor.fetchone()
                if artist:
                    artist = dict(artist)
                    cursor.execute('SELECT DISTINCT genre FROM artists WHERE genre != ? ORDER BY RANDOM() LIMIT 3',
                                   (artist['genre'],))
                    wrong_genres = [row[0] for row in cursor.fetchall()]
                    options = [artist['genre']] + wrong_genres
                    random.shuffle(options)
                    return {
                        'type': 'genre',
                        'question': f"К какому жанру относится {artist['name']}?",
                        'correct_answer': artist['genre'],
                        'options': options,
                        'artist_id': artist['id']
                    }
            elif question_type == "artist":
                cursor.execute('SELECT id, name, facts FROM artists ORDER BY RANDOM() LIMIT 1')
                artist = cursor.fetchone()
                if artist:
                    artist = dict(artist)
                    cursor.execute('SELECT name FROM artists WHERE id != ? ORDER BY RANDOM() LIMIT 3', (artist['id'],))
                    wrong_names = [row[0] for row in cursor.fetchall()]
                    options = [artist['name']] + wrong_names
                    random.shuffle(options)
                    fact = artist['facts'][:150] + "..." if len(artist['facts']) > 150 else artist['facts']
                    return {
                        'type': 'artist',
                        'question': f"О ком идет речь?\n\n{fact}",
                        'correct_answer': artist['name'],
                        'options': options,
                        'artist_id': artist['id']
                    }
            elif question_type == "song":
                cursor.execute('SELECT id, name, hits FROM artists WHERE hits IS NOT NULL AND hits != "" ORDER BY RANDOM() LIMIT 1')
                artist = cursor.fetchone()
                if artist:
                    artist = dict(artist)
                    hits = artist['hits'].split(',')
                    if hits:
                        song = random.choice([h.strip() for h in hits if h.strip()])
                        cursor.execute('SELECT hits FROM artists WHERE id != ? AND hits IS NOT NULL AND hits != "" ORDER BY RANDOM() LIMIT 3', (artist['id'],))
                        other_hits = []
                        for row in cursor.fetchall():
                            songs = row[0].split(',')
                            if songs:
                                other_hits.append(random.choice([s.strip() for s in songs if s.strip()]))
                        options = [song] + other_hits[:3]
                        random.shuffle(options)
                        return {
                            'type': 'song',
                            'question': f"Какая песня принадлежит {artist['name']}?",
                            'correct_answer': song,
                            'options': options,
                            'artist_id': artist['id']
                        }
            conn.close()
            return None

    # ---- Методы для пользователей ----
    def init_user(self, user_id, username):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)',
                           (user_id, username or f"user_{user_id}"))
            cursor.execute('UPDATE users SET last_seen = CURRENT_TIMESTAMP, username = COALESCE(?, username) WHERE user_id = ?',
                           (username, user_id))
            conn.commit()
            conn.close()

    def log_search(self, user_id, query, artist_found=False, artist_id=None):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO search_history (user_id, query, artist_found, artist_id) VALUES (?, ?, ?, ?)',
                           (user_id, query, artist_found, artist_id))
            if artist_found:
                cursor.execute('UPDATE users SET searches_count = searches_count + 1 WHERE user_id = ?', (user_id,))
            conn.commit()
            conn.close()

    def log_quiz_result(self, user_id, quiz_type, score, questions_played):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET quizzes_played = quizzes_played + 1, quiz_points = quiz_points + ? WHERE user_id = ?',
                           (score, user_id))
            cursor.execute('SELECT best_quiz_score FROM users WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()
            current_best = result[0] if result else 0
            if score > current_best:
                cursor.execute('UPDATE users SET best_quiz_score = ? WHERE user_id = ?', (score, user_id))
            cursor.execute('INSERT INTO quiz_results (user_id, quiz_type, score, questions_played) VALUES (?, ?, ?, ?)',
                           (user_id, quiz_type, score, questions_played))
            conn.commit()
            conn.close()

    def get_user_stats(self, user_id):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT username, searches_count, quizzes_played, quiz_points, best_quiz_score, favorites_count FROM users WHERE user_id = ?', (user_id,))
            user_data = cursor.fetchone()
            if not user_data:
                conn.close()
                return None
            cursor.execute('SELECT query, COUNT(*) as count FROM search_history WHERE user_id = ? AND artist_found = 1 GROUP BY query ORDER BY count DESC LIMIT 5', (user_id,))
            top_searches = cursor.fetchall()
            cursor.execute('SELECT a.genre, COUNT(*) as count FROM search_history sh JOIN artists a ON sh.artist_id = a.id WHERE sh.user_id = ? AND sh.artist_found = 1 GROUP BY a.genre ORDER BY count DESC LIMIT 1', (user_id,))
            favorite_genre_result = cursor.fetchone()
            conn.close()
            return {
                'username': user_data[0] or 'Аноним',
                'searches': user_data[1],
                'quizzes_played': user_data[2],
                'quiz_points': user_data[3],
                'best_quiz_score': user_data[4],
                'favorites': user_data[5],
                'top_searches': [(row[0], row[1]) for row in top_searches],
                'favorite_genre': favorite_genre_result[0] if favorite_genre_result else "Не определен"
            }

    def get_leaderboard(self, metric='quiz_points', limit=10):
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            if metric == 'quiz_points':
                cursor.execute('SELECT COALESCE(username, "Аноним") as name, quiz_points FROM users WHERE quiz_points > 0 ORDER BY quiz_points DESC LIMIT ?', (limit,))
            elif metric == 'searches':
                cursor.execute('SELECT COALESCE(username, "Аноним") as name, searches_count FROM users WHERE searches_count > 0 ORDER BY searches_count DESC LIMIT ?', (limit,))
            elif metric == 'quizzes':
                cursor.execute('SELECT COALESCE(username, "Аноним") as name, quizzes_played FROM users WHERE quizzes_played > 0 ORDER BY quizzes_played DESC LIMIT ?', (limit,))
            else:
                cursor.execute('SELECT COALESCE(username, "Аноним") as name, (quiz_points * 1.5 + searches_count * 0.5 + quizzes_played * 5) as activity_score FROM users ORDER BY activity_score DESC LIMIT ?', (limit,))
            results = cursor.fetchall()
            conn.close()
            return [(row[0], row[1]) for row in results]


# Инициализация базы данных
print("🔄 Инициализация базы данных...")
db = MusicDatabase()


# ---- Вспомогательные функции ----
def format_number(num):
    if not num:
        return "Нет данных"
    try:
        num = int(num)
        if num >= 1000000:
            return f"{num/1000000:.1f}M"
        elif num >= 1000:
            return f"{num/1000:.1f}K"
        return str(num)
    except (ValueError, TypeError):
        return str(num)


def format_artist_info(artist):
    if not artist:
        return "Артист не найден."
    text = f"<b>{artist['name']}</b>\n"
    if artist.get('full_name'):
        text += f"<i>{artist['full_name']}</i>\n\n"
    text += f"🎭 <b>Жанр:</b> {artist['genre']}"
    if artist.get('subgenre'):
        text += f" ({artist['subgenre']})"
    text += "\n"
    text += f"🌍 <b>Страна:</b> {artist['country']}"
    if artist.get('city'):
        text += f", {artist['city']}"
    text += "\n"
    text += f"📅 <b>Годы активности:</b> {artist['years']}\n"
    if artist.get('birth_date'):
        text += f"🎂 <b>Дата рождения:</b> {artist['birth_date']}\n"
    if artist.get('formation_year') and not artist.get('is_solo_artist'):
        text += f"🏛️ <b>Основан:</b> {artist['formation_year']}\n"
    elif artist.get('career_start_year'):
        text += f"🎤 <b>Начало карьеры:</b> {artist['career_start_year']}\n"
    if artist.get('members'):
        members = artist['members']
        if len(members) > 100:
            members = members[:100] + "..."
        text += f"👥 <b>Состав:</b> {members}\n"
    text += f"\n📊 <b>Статистика:</b>\n"
    text += f"🏆 <b>Влияние:</b> {artist['influence_rating']}/10\n"
    if artist.get('grammy_awards') and artist['grammy_awards'] > 0:
        text += f"🎖️ <b>Grammy Awards:</b> {artist['grammy_awards']}\n"
    if artist.get('spotify_listeners') and artist['spotify_listeners'] > 0:
        text += f"🎵 <b>Spotify:</b> {format_number(artist['spotify_listeners'])} слушателей/мес\n"
    if artist.get('youtube_subscribers') and artist['youtube_subscribers'] > 0:
        text += f"📺 <b>YouTube:</b> {format_number(artist['youtube_subscribers'])} подписчиков\n"
    if artist.get('album_count') and artist['album_count'] > 0:
        text += f"💿 <b>Альбомов:</b> {artist['album_count']}\n"
    if artist.get('album_sales'):
        text += f"💰 <b>Продажи:</b> {artist['album_sales']}\n"
    text += f"\n🔥 <b>Главные хиты:</b>\n{artist['hits']}\n\n"
    if artist.get('signature_style'):
        text += f"🎨 <b>Стиль:</b> {artist['signature_style']}\n\n"
    text += f"💡 <b>Интересные факты:</b>\n{artist['facts']}"
    return text


# ---- Обработчики команд и сообщений ----
@bot.message_handler(commands=['start'])
def start_command(message):
    user = message.from_user
    db.init_user(user.id, user.username or user.first_name)
    log_action(user.id, user.username, "/start")

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        "🎲 Случайный артист",
        "🎵 Поиск по жанрам",
        "🔍 Поиск по имени",
        "🎮 Музыкальная викторина",
        "🔄 Сравнить артистов",
        "📊 Моя статистика",
        "🏆 Таблица лидеров",
        "ℹ️ О боте"
    ]
    for btn in buttons:
        markup.add(types.KeyboardButton(btn))

    bot.send_message(
        message.chat.id,
        "🎶 <b>Голос времени - Музыкальная энциклопедия</b>\n\n"
        "Добро пожаловать! Я помогу вам узнать всё о ваших любимых артистах.\n\n"
        "<b>Доступные функции:</b>\n"
        "• 🎲 Случайный артист - откройте для себя нового исполнителя\n"
        "• 🎵 Поиск по жанрам - найдите артистов по музыкальному стилю\n"
        "• 🔍 Поиск по имени - ищите конкретных исполнителей\n"
        "• 🎮 Музыкальная викторина - проверьте свои знания\n"
        "• 🔄 Сравнить артистов - узнайте сходства и различия\n"
        "• 📊 Моя статистика - отслеживайте свою активность\n"
        "• 🏆 Таблица лидеров - соревнуйтесь с другими пользователями\n\n"
        "Выберите действие:",
        parse_mode='HTML',
        reply_markup=markup
    )


@bot.message_handler(commands=['help'])
def help_command(message):
    user = message.from_user
    log_action(user.id, user.username, "/help")
    help_text = """
🎶 <b>Голос времени - Музыкальная энциклопедия</b>

<b>📚 О проекте:</b>
Музыкальный бот с базой данных популярных исполнителей разных эпох и жанров. От классики до современных хитов!

<b>✨ Как работает бот:</b>

<b>1. 🎲 Случайный артист</b>
• Получайте случайного исполнителя из базы
• Изучайте подробную информацию о нем
• Открывайте для себя новую музыку

<b>2. 🎵 Поиск по жанрам</b>
• Выбирайте из 12+ музыкальных жанров
• Смотрите список артистов в каждом жанре
• Переходите к подробной информации

<b>3. 🔍 Поиск по имени</b>
• Ищите артистов по имени или фамилии
• Находите как классических, так и современных исполнителей
• Получайте полную информацию о каждом

<b>4. 🎮 Музыкальная викторина</b>
• 3 типа вопросов: жанр, артист, песня
• 5 вопросов в каждой викторине
• Зарабатывайте очки и поднимайтесь в таблице лидеров
• Соревнуйтесь с другими пользователями

<b>5. 🔄 Сравнение артистов</b>
• Сравнивайте двух любых артистов
• Анализируйте сходства и различия
• Смотрите статистику популярности и влияния

<b>6. 📊 Статистика</b>
• Отслеживайте свою активность
• Смотрите историю поисков
• Анализируйте свои результаты в викторинах

<b>7. 🏆 Таблица лидеров</b>
• Соревнуйтесь с другими пользователями
• 4 категории: очки викторины, поиски, викторины, общая активность
• Станьте лучшим музыкальным экспертом!

<b>📱 Управление:</b>
• Используйте кнопки меню для навигации
• Для поиска просто введите имя артиста
• В викторине выбирайте ответы на кнопках
• Для сравнения используйте формат: "Артист1 vs Артист2"

<b>🎵 База данных:</b>
• 50+ популярных артистов
• 12+ музыкальных жанров
• Полная информация по каждому исполнителю
• Регулярные обновления базы

<b>💡 Советы:</b>
• Чаще играйте в викторину для улучшения знаний
• Используйте сравнение для изучения музыкальных стилей
• Сохраняйте любимых артистов в избранное
• Следите за своей статистикой

Для начала работы используйте команду /start или выберите действие из меню!
"""
    bot.send_message(message.chat.id, help_text, parse_mode='HTML')


@bot.message_handler(func=lambda message: message.text == "🎲 Случайный артист")
def random_artist(message):
    user = message.from_user
    log_action(user.id, user.username, "Случайный артист")
    artist = db.get_random_artist()
    if artist:
        text = format_artist_info(artist)
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🎮 Викторина", callback_data=f"quiz_{artist['id']}"),
            types.InlineKeyboardButton("🔄 Сравнить", callback_data=f"compare_{artist['id']}")
        )
        bot.send_message(message.chat.id, text, parse_mode='HTML', reply_markup=markup)
    else:
        bot.send_message(message.chat.id, "Не удалось найти артиста. Попробуйте позже.")


@bot.message_handler(func=lambda message: message.text == "🎵 Поиск по жанрам")
def show_genres(message):
    user = message.from_user
    log_action(user.id, user.username, "Поиск по жанрам (меню)")
    genres = db.get_all_genres()
    markup = types.InlineKeyboardMarkup(row_width=2)
    for genre in genres:
        markup.add(types.InlineKeyboardButton(genre, callback_data=f"genre_{genre}"))
    bot.send_message(
        message.chat.id,
        "🎭 <b>Выберите музыкальный жанр:</b>",
        parse_mode='HTML',
        reply_markup=markup
    )


@bot.message_handler(func=lambda message: message.text == "🔍 Поиск по имени")
def search_by_name(message):
    user = message.from_user
    log_action(user.id, user.username, "Поиск по имени (начало)")
    msg = bot.send_message(
        message.chat.id,
        "Введите имя артиста или группы:",
        reply_markup=types.ReplyKeyboardRemove()
    )
    bot.register_next_step_handler(msg, process_search)


def process_search(message):
    user = message.from_user
    query = message.text.strip()
    if not query:
        bot.send_message(message.chat.id, "Пожалуйста, введите имя артиста.")
        return
    artist = db.search_artist(query)
    if artist:
        db.log_search(user.id, query, True, artist['id'])
        log_action(user.id, user.username, f"Поиск: '{query}'", "успешно")
        text = format_artist_info(artist)
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🎮 Викторина", callback_data=f"quiz_{artist['id']}"),
            types.InlineKeyboardButton("🔄 Сравнить", callback_data=f"compare_{artist['id']}")
        )
        markup.add(
            types.InlineKeyboardButton("🎵 Похожие артисты", callback_data=f"similar_{artist['id']}"),
            types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
        )
        bot.send_message(message.chat.id, text, parse_mode='HTML', reply_markup=markup)
    else:
        db.log_search(user.id, query, False)
        log_action(user.id, user.username, f"Поиск: '{query}'", "не найдено")
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("🔍 Новый поиск", callback_data="new_search"),
            types.InlineKeyboardButton("🎵 По жанрам", callback_data="show_genres_menu")
        )
        bot.send_message(
            message.chat.id,
            f"Артист '{query}' не найден в базе данных.\n\n"
            "Попробуйте:\n"
            "• Проверить написание\n"
            "• Использовать только фамилию\n"
            "• Поискать по жанру",
            reply_markup=markup
        )


@bot.message_handler(func=lambda message: message.text == "🎮 Музыкальная викторина")
def show_quiz_menu(message):
    user = message.from_user
    log_action(user.id, user.username, "Меню викторины")
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🎭 Угадай жанр", callback_data="quiz_type_genre"),
        types.InlineKeyboardButton("👤 Угадай артиста", callback_data="quiz_type_artist")
    )
    markup.add(
        types.InlineKeyboardButton("🎵 Угадай песню", callback_data="quiz_type_song"),
        types.InlineKeyboardButton("🎲 Случайная викторина", callback_data="quiz_type_random")
    )
    markup.add(
        types.InlineKeyboardButton("📊 Статистика викторины", callback_data="quiz_stats")
    )
    bot.send_message(
        message.chat.id,
        "🎮 <b>Музыкальная викторина</b>\n\n"
        "Выберите тип вопросов:\n\n"
        "<b>🎭 Угадай жанр</b> - определи жанр артиста\n"
        "<b>👤 Угадай артиста</b> - узнай артиста по факту\n"
        "<b>🎵 Угадай песню</b> - найди песню артиста\n"
        "<b>🎲 Случайная</b> - все типы вопросов",
        parse_mode='HTML',
        reply_markup=markup
    )


@bot.message_handler(func=lambda message: message.text == "🔄 Сравнить артистов")
def start_comparison(message):
    user = message.from_user
    log_action(user.id, user.username, "Меню сравнения")
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("🎲 Случайные артисты", callback_data="compare_random"),
        types.InlineKeyboardButton("🔍 Выбрать артистов", callback_data="compare_select")
    )
    bot.send_message(
        message.chat.id,
        "🔄 <b>Сравнение артистов</b>\n\n"
        "Вы можете сравнить:\n"
        "• Двух случайных артистов\n"
        "• Выбрать артистов самостоятельно",
        parse_mode='HTML',
        reply_markup=markup
    )


@bot.message_handler(func=lambda message: message.text == "📊 Моя статистика")
def show_user_stats(message):
    user = message.from_user
    log_action(user.id, user.username, "Моя статистика")
    stats = db.get_user_stats(user.id)
    if not stats:
        bot.send_message(message.chat.id, "У вас еще нет статистики. Начните пользоваться ботом!")
        return
    text = f"📊 <b>Ваша статистика</b>\n\n"
    text += f"👤 <b>Пользователь:</b> {stats['username']}\n"
    text += f"🔍 <b>Поисков:</b> {stats['searches']}\n"
    text += f"🎮 <b>Сыграно викторин:</b> {stats['quizzes_played']}\n"
    text += f"🏆 <b>Очков в викторинах:</b> {stats['quiz_points']}\n"
    text += f"👑 <b>Лучший результат:</b> {stats['best_quiz_score']}\n"
    text += f"❤️ <b>Избранных артистов:</b> {stats['favorites']}\n"
    text += f"🎭 <b>Любимый жанр:</b> {stats['favorite_genre']}\n\n"
    if stats['top_searches']:
        text += "<b>🔍 Чаще всего искали:</b>\n"
        for query, count in stats['top_searches']:
            text += f"• {query} - {count} раз\n"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
    bot.send_message(message.chat.id, text, parse_mode='HTML', reply_markup=markup)


@bot.message_handler(func=lambda message: message.text == "🏆 Таблица лидеров")
def show_leaderboard_menu(message):
    user = message.from_user
    log_action(user.id, user.username, "Таблица лидеров (меню)")
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🏆 По очкам викторины", callback_data="leaderboard_points"),
        types.InlineKeyboardButton("🔍 По поискам", callback_data="leaderboard_searches")
    )
    markup.add(
        types.InlineKeyboardButton("🎮 По викторинам", callback_data="leaderboard_quizzes"),
        types.InlineKeyboardButton("📊 Общая активность", callback_data="leaderboard_all")
    )
    bot.send_message(
        message.chat.id,
        "🏆 <b>Таблица лидеров</b>\n\n"
        "Выберите категорию для рейтинга:",
        parse_mode='HTML',
        reply_markup=markup
    )


@bot.message_handler(func=lambda message: message.text == "ℹ️ О боте")
def about_bot(message):
    user = message.from_user
    log_action(user.id, user.username, "О боте")
    text = """
🎶 <b>Голос времени - Музыкальная энциклопедия</b>

<b>📱 О боте:</b>
Интеллектуальный музыкальный бот с обширной базой данных артистов. Идеальный помощник для изучения музыки!

<b>🎵 Основные возможности:</b>
• База из 50+ популярных артистов
• 12+ музыкальных жанров
• Интерактивная викторина с 3 типами вопросов
• Детальное сравнение исполнителей
• Система статистики и достижений
• Таблица лидеров

<b>🔍 Поисковые возможности:</b>
• Поиск по имени артиста
• Поиск по жанрам
• Умный поиск с учетом альтернативных названий
• История поисковых запросов

<b>🎮 Викторина:</b>
• Три режима: жанр, артист, песня
• Система очков и рейтингов
• Прогрессивная сложность вопросов
• Личная статистика

<b>📊 Аналитика:</b>
• Подробная статистика по каждому артисту
• Сравнение популярности и влияния
• Анализ музыкальных стилей
• Рекомендации похожих артистов

<b>💾 Технические характеристики:</b>
• Быстрый и стабильный поиск
• Интуитивный интерфейс
• Регулярные обновления базы
• Оптимизированная работа

<b>🎯 Для кого этот бот:</b>
• Музыкальные энтузиасты
• Студенты музыкальных школ
• DJ и музыканты
• Все, кто хочет расширить музыкальный кругозор

<b>🚀 Начните прямо сейчас!</b>
Используйте меню для навигации или команду /help для получения подробной информации.
"""
    bot.send_message(message.chat.id, text, parse_mode='HTML')


# ---- Callback обработчик ----
@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    user_id = call.from_user.id
    username = call.from_user.username
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    data = call.data

    # Логируем callback (кроме ответов на вопросы, чтобы не спамить)
    if not data.startswith("answer_"):
        if data.startswith("genre_"):
            genre = data.replace("genre_", "")
            log_action(user_id, username, f"Выбор жанра: {genre}")
        elif data.startswith("artist_"):
            artist_id = data.replace("artist_", "")
            log_action(user_id, username, f"Просмотр артиста ID {artist_id}")
        elif data.startswith("quiz_type_"):
            qtype = data.replace("quiz_type_", "")
            log_action(user_id, username, f"Начало викторины: {qtype}")
        elif data == "quiz_stats":
            log_action(user_id, username, "Просмотр статистики викторины")
        elif data.startswith("compare_"):
            if data == "compare_random":
                log_action(user_id, username, "Сравнение случайных артистов")
            elif data == "compare_select":
                log_action(user_id, username, "Сравнение (выбор артистов)")
            else:
                log_action(user_id, username, "Сравнение с выбранным артистом")
        elif data.startswith("leaderboard_"):
            metric = data.replace("leaderboard_", "")
            log_action(user_id, username, f"Просмотр таблицы лидеров: {metric}")
        elif data.startswith("similar_"):
            log_action(user_id, username, "Просмотр похожих артистов")
        elif data == "main_menu":
            log_action(user_id, username, "Возврат в главное меню")
        elif data == "new_search":
            log_action(user_id, username, "Новый поиск (с кнопки)")
        elif data == "show_genres_menu":
            log_action(user_id, username, "Возврат к жанрам")

    # Обработка различных callback_data
    try:
        if data == "main_menu":
            start_command(call.message)
        elif data == "new_search":
            search_by_name(call.message)
        elif data == "show_genres_menu":
            show_genres(call.message)
        elif data.startswith("genre_"):
            genre = data.replace("genre_", "")
            artists = db.get_artists_by_genre(genre)
            if not artists:
                bot.answer_callback_query(call.id, "В этом жанре пока нет артистов")
                return
            text = f"🎭 <b>{genre}</b>\n\nАртисты:\n"
            markup = types.InlineKeyboardMarkup(row_width=2)
            for artist in artists:
                text += f"• {artist['name']}\n"
                markup.add(types.InlineKeyboardButton(artist['name'], callback_data=f"artist_{artist['id']}"))
            markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="show_genres_menu"))
            bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
        elif data.startswith("artist_"):
            artist_id = int(data.replace("artist_", ""))
            artist = db.get_artist_by_id(artist_id)
            if artist:
                text = format_artist_info(artist)
                markup = types.InlineKeyboardMarkup(row_width=2)
                markup.add(
                    types.InlineKeyboardButton("🎮 Викторина", callback_data=f"quiz_{artist_id}"),
                    types.InlineKeyboardButton("🔄 Сравнить", callback_data=f"compare_{artist_id}")
                )
                markup.add(
                    types.InlineKeyboardButton("🎵 Похожие артисты", callback_data=f"similar_{artist_id}"),
                    types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
                )
                bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
            else:
                bot.answer_callback_query(call.id, "Артист не найден")
        elif data.startswith("quiz_"):
            if data.startswith("quiz_type_"):
                quiz_type = data.replace("quiz_type_", "")
                if quiz_type == "random":
                    quiz_type = random.choice(["genre", "artist", "song"])
                start_quiz_session(chat_id, message_id, quiz_type, user_id)
            elif data == "quiz_stats":
                stats = db.get_user_stats(user_id)
                if stats and stats['quizzes_played'] > 0:
                    text = f"📊 <b>Ваша статистика викторины</b>\n\n"
                    text += f"🎮 <b>Сыграно:</b> {stats['quizzes_played']}\n"
                    text += f"🏆 <b>Всего очков:</b> {stats['quiz_points']}\n"
                    text += f"👑 <b>Лучший результат:</b> {stats['best_quiz_score']}\n"
                    avg_score = stats['quiz_points'] / stats['quizzes_played'] if stats['quizzes_played'] > 0 else 0
                    text += f"📈 <b>Средний балл:</b> {avg_score:.1f}\n\n"
                    if avg_score >= 8:
                        text += "🎸 <b>Вы настоящий музыкальный эксперт!</b>"
                    elif avg_score >= 6:
                        text += "🎵 <b>Отличные знания музыки!</b>"
                    elif avg_score >= 4:
                        text += "🎧 <b>Хорошие знания, продолжайте в том же духе!</b>"
                    else:
                        text += "🎶 <b>Попробуйте еще раз, у вас все получится!</b>"
                    bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML')
                else:
                    bot.answer_callback_query(call.id, "Вы еще не играли в викторину")
            else:
                artist_id = int(data.replace("quiz_", ""))
                artist = db.get_artist_by_id(artist_id)
                if artist:
                    start_artist_quiz(chat_id, message_id, artist, user_id)
        elif data.startswith("compare_"):
            if data == "compare_random":
                artist1 = db.get_random_artist()
                artist2 = db.get_random_artist()
                while artist2 and artist2['id'] == artist1['id']:
                    artist2 = db.get_random_artist()
                if artist1 and artist2:
                    show_comparison(chat_id, message_id, artist1, artist2)
                else:
                    bot.answer_callback_query(call.id, "Не удалось найти артистов для сравнения")
            elif data == "compare_select":
                msg = bot.send_message(chat_id, "Введите имена двух артистов через 'vs':\nПример: <code>Queen vs The Beatles</code>", parse_mode='HTML')
                bot.register_next_step_handler(msg, process_comparison_input)
            else:
                artist1_id = int(data.replace("compare_", ""))
                artist1 = db.get_artist_by_id(artist1_id)
                if artist1:
                    msg = bot.send_message(chat_id, f"Вы выбрали {artist1['name']}.\nТеперь введите имя второго артиста для сравнения:")
                    bot.register_next_step_handler(msg, lambda m: process_comparison_with_first(m, artist1_id))
        elif data.startswith("leaderboard_"):
            metric = data.replace("leaderboard_", "")
            if metric == "points":
                leaderboard = db.get_leaderboard('quiz_points', 10)
                title = "🏆 <b>Таблица лидеров по очкам в викторине</b>\n\n"
                emoji = "🏆"
            elif metric == "searches":
                leaderboard = db.get_leaderboard('searches', 10)
                title = "🔍 <b>Таблица лидеров по поискам</b>\n\n"
                emoji = "🔍"
            elif metric == "quizzes":
                leaderboard = db.get_leaderboard('quizzes', 10)
                title = "🎮 <b>Таблица лидеров по викторинам</b>\n\n"
                emoji = "🎮"
            else:
                leaderboard = db.get_leaderboard('all', 10)
                title = "📊 <b>Общая таблица лидеров (активность)</b>\n\n"
                emoji = "📊"
            if leaderboard:
                medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
                text = title
                for i, (username, score) in enumerate(leaderboard):
                    medal = medals[i] if i < len(medals) else f"{i+1}."
                    if metric == "all":
                        text += f"{medal} {username}: {score:.1f} баллов\n"
                    else:
                        text += f"{medal} {username}: {score} {emoji}\n"
            else:
                text = "Пока нет данных для таблицы лидеров.\nБудьте первым!"
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton("🏆 Очки", callback_data="leaderboard_points"),
                types.InlineKeyboardButton("🔍 Поиски", callback_data="leaderboard_searches"),
                types.InlineKeyboardButton("🎮 Викторины", callback_data="leaderboard_quizzes"),
                types.InlineKeyboardButton("📊 Общая", callback_data="leaderboard_all")
            )
            bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
        elif data.startswith("similar_"):
            artist_id = int(data.replace("similar_", ""))
            artist = db.get_artist_by_id(artist_id)
            if artist and artist.get('similar_artists'):
                similar = artist['similar_artists']
                text = f"🎵 <b>Артисты, похожие на {artist['name']}</b>\n\n{similar}\n\n<i>Нажмите на имя артиста для подробной информации</i>"
                markup = types.InlineKeyboardMarkup()
                similar_list = [s.strip() for s in similar.split(',')]
                for name in similar_list[:4]:
                    similar_artist = db.search_artist(name)
                    if similar_artist:
                        markup.add(types.InlineKeyboardButton(f"🎵 {similar_artist['name']}", callback_data=f"artist_{similar_artist['id']}"))
                markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"artist_{artist_id}"))
                bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
            else:
                bot.answer_callback_query(call.id, "Нет информации о похожих артистах")
        elif data.startswith("answer_"):
            process_quiz_answer(call)
    except Exception as e:
        print(f"Ошибка в callback: {e}")
        bot.answer_callback_query(call.id, "Произошла ошибка")
        import traceback
        traceback.print_exc()


def process_comparison_input(message):
    user = message.from_user
    text = message.text.strip()
    if ' vs ' in text.lower():
        parts = text.split(' vs ')
    elif ' против ' in text.lower():
        parts = text.split(' против ')
    else:
        bot.send_message(message.chat.id, "Используйте формат: 'Артист1 vs Артист2'")
        log_action(user.id, user.username, f"Сравнение: неверный формат", "неудачно")
        return
    if len(parts) != 2:
        bot.send_message(message.chat.id, "Нужно указать ровно двух артистов")
        log_action(user.id, user.username, f"Сравнение: неверное количество артистов", "неудачно")
        return
    artist1_name = parts[0].strip()
    artist2_name = parts[1].strip()
    artist1 = db.search_artist(artist1_name)
    artist2 = db.search_artist(artist2_name)
    if not artist1 or not artist2:
        not_found = []
        if not artist1: not_found.append(artist1_name)
        if not artist2: not_found.append(artist2_name)
        bot.send_message(message.chat.id, f"Не найдены артисты: {', '.join(not_found)}")
        log_action(user.id, user.username, f"Сравнение: не найдены {', '.join(not_found)}", "неудачно")
        return
    log_action(user.id, user.username, f"Сравнение: {artist1['name']} vs {artist2['name']}", "успешно")
    show_comparison(message.chat.id, None, artist1, artist2)


def process_comparison_with_first(message, artist1_id):
    user = message.from_user
    artist2_name = message.text.strip()
    artist1 = db.get_artist_by_id(artist1_id)
    artist2 = db.search_artist(artist2_name)
    if not artist2:
        bot.send_message(message.chat.id, f"Артист '{artist2_name}' не найден")
        log_action(user.id, user.username, f"Сравнение: второй артист '{artist2_name}' не найден", "неудачно")
        return
    log_action(user.id, user.username, f"Сравнение: {artist1['name']} vs {artist2['name']}", "успешно")
    show_comparison(message.chat.id, None, artist1, artist2)


def show_comparison(chat_id, message_id, artist1, artist2):
    comparison = db.compare_artists(artist1['id'], artist2['id'])
    if not comparison:
        text = "Не удалось сравнить артистов."
        if message_id:
            bot.edit_message_text(text, chat_id, message_id)
        else:
            bot.send_message(chat_id, text)
        return
    text = f"🔄 <b>Сравнение: {artist1['name']} vs {artist2['name']}</b>\n\n"
    text += f"<b>{artist1['name']}</b>\n"
    text += f"🎭 Жанр: {artist1['genre']}\n"
    text += f"🌍 Страна: {artist1['country']}\n"
    text += f"📅 Годы: {artist1['years']}\n"
    text += f"🏆 Влияние: {artist1['influence_rating']}/10\n"
    if artist1['spotify_listeners']:
        text += f"🎵 Spotify: {format_number(artist1['spotify_listeners'])}\n"
    if artist1['grammy_awards']:
        text += f"🏆 Grammy: {artist1['grammy_awards']}\n"
    text += f"\n<b>{artist2['name']}</b>\n"
    text += f"🎭 Жанр: {artist2['genre']}\n"
    text += f"🌍 Страна: {artist2['country']}\n"
    text += f"📅 Годы: {artist2['years']}\n"
    text += f"🏆 Влияние: {artist2['influence_rating']}/10\n"
    if artist2['spotify_listeners']:
        text += f"🎵 Spotify: {format_number(artist2['spotify_listeners'])}\n"
    if artist2['grammy_awards']:
        text += f"🏆 Grammy: {artist2['grammy_awards']}\n"
    text += f"\n📊 <b>Анализ сравнения:</b>\n\n"
    if comparison['similarities']:
        text += "✅ <b>Сходства:</b>\n"
        for similarity in comparison['similarities']:
            text += f"• {similarity}\n"
    if comparison['differences']:
        text += "\n🔀 <b>Различия:</b>\n"
        for difference in comparison['differences']:
            text += f"• {difference}\n"
    if artist1['influence_rating'] > artist2['influence_rating']:
        winner = artist1['name']
    elif artist2['influence_rating'] > artist1['influence_rating']:
        winner = artist2['name']
    else:
        winner = "Оба артиста равны по влиянию"
    text += f"\n🏆 <b>По влиянию лидирует:</b> {winner}"
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton(f"📖 {artist1['name']}", callback_data=f"artist_{artist1['id']}"),
        types.InlineKeyboardButton(f"📖 {artist2['name']}", callback_data=f"artist_{artist2['id']}")
    )
    markup.add(
        types.InlineKeyboardButton("🔄 Новое сравнение", callback_data="compare_random"),
        types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
    )
    if message_id:
        bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
    else:
        bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)


# ---- Функции викторины ----
def start_quiz_session(chat_id, message_id, quiz_type, user_id):
    active_quizzes[chat_id] = {
        'type': quiz_type,
        'questions': [],
        'current_question': 0,
        'score': 0,
        'user_id': user_id,
        'total_questions': 5,
        'message_id': message_id
    }
    for _ in range(5):
        question = db.get_quiz_question(quiz_type)
        if question:
            active_quizzes[chat_id]['questions'].append(question)
        else:
            question = db.get_quiz_question("genre")
            if question:
                active_quizzes[chat_id]['questions'].append(question)
    if not active_quizzes[chat_id]['questions']:
        del active_quizzes[chat_id]
        bot.answer_callback_query(message_id, "Не удалось загрузить вопросы")
        return
    show_quiz_question(chat_id, message_id)


def start_artist_quiz(chat_id, message_id, artist, user_id):
    questions = []
    if artist.get('formation_year') and not artist.get('is_solo_artist'):
        questions.append({
            'type': 'year',
            'question': f"В каком году была основана группа {artist['name']}?",
            'correct_answer': str(artist['formation_year']),
            'options': generate_year_options(artist['formation_year']),
            'artist_id': artist['id']
        })
    elif artist.get('career_start_year') and artist.get('is_solo_artist'):
        questions.append({
            'type': 'year',
            'question': f"В каком году начал свою музыкальную карьеру {artist['name']}?",
            'correct_answer': str(artist['career_start_year']),
            'options': generate_year_options(artist['career_start_year']),
            'artist_id': artist['id']
        })
    questions.append({
        'type': 'country',
        'question': f"Из какой страны артист {artist['name']}?",
        'correct_answer': artist['country'],
        'options': generate_country_options(artist['country']),
        'artist_id': artist['id']
    })
    questions.append({
        'type': 'genre',
        'question': f"К какому жанру относится {artist['name']}?",
        'correct_answer': artist['genre'],
        'options': generate_genre_options(artist['genre']),
        'artist_id': artist['id']
    })
    if len(questions) < 2:
        bot.answer_callback_query(message_id, "Недостаточно данных для викторины")
        return
    active_quizzes[chat_id] = {
        'type': 'artist_quiz',
        'questions': questions,
        'current_question': 0,
        'score': 0,
        'user_id': user_id,
        'total_questions': len(questions),
        'artist_id': artist['id'],
        'message_id': message_id
    }
    show_quiz_question(chat_id, message_id)


def generate_year_options(correct_year):
    options = [str(correct_year)]
    for _ in range(3):
        offset = random.randint(-15, 15)
        while offset == 0:
            offset = random.randint(-15, 15)
        options.append(str(correct_year + offset))
    random.shuffle(options)
    return options


def generate_country_options(correct_country):
    countries = ["США", "Великобритания", "Канада", "Австралия", "Германия", "Франция",
                 "Швеция", "Италия", "Испания", "Япония", "Южная Корея", "Бразилия", "Мексика"]
    options = [correct_country]
    wrong = [c for c in countries if c != correct_country]
    options.extend(random.sample(wrong, min(3, len(wrong))))
    random.shuffle(options)
    return options


def generate_genre_options(correct_genre):
    genres = db.get_all_genres()
    options = [correct_genre]
    wrong = [g for g in genres if g != correct_genre]
    options.extend(random.sample(wrong, min(3, len(wrong))))
    random.shuffle(options)
    return options


def show_quiz_question(chat_id, message_id):
    if chat_id not in active_quizzes:
        return
    quiz = active_quizzes[chat_id]
    if quiz['current_question'] >= len(quiz['questions']):
        finish_quiz(chat_id, message_id)
        return
    question = quiz['questions'][quiz['current_question']]
    q_num = quiz['current_question'] + 1
    total = quiz['total_questions']
    text = f"🎮 <b>Вопрос {q_num}/{total}</b>\n\n{question['question']}\n\n"
    markup = types.InlineKeyboardMarkup(row_width=2)
    for i, opt in enumerate(question['options'], 1):
        markup.add(types.InlineKeyboardButton(f"{i}. {opt}", callback_data=f"answer_{opt}"))
    try:
        bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
    except:
        new_msg = bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
        quiz['message_id'] = new_msg.message_id


def process_quiz_answer(call):
    chat_id = call.message.chat.id
    if chat_id not in active_quizzes:
        bot.answer_callback_query(call.id, "Викторина завершена")
        return
    quiz = active_quizzes[chat_id]
    question = quiz['questions'][quiz['current_question']]
    user_answer = call.data.replace("answer_", "")
    correct = (user_answer == question['correct_answer'])
    if correct:
        quiz['score'] += 10
        result = f"✅ <b>Правильно!</b> +10 очков\n"
    else:
        result = f"❌ <b>Неправильно.</b> Правильный ответ: {question['correct_answer']}\n"
    quiz['current_question'] += 1
    if quiz['current_question'] < len(quiz['questions']):
        result += f"\n🏆 Текущий счет: {quiz['score']} очков"
        try:
            bot.edit_message_text(result, chat_id, call.message.message_id, parse_mode='HTML')
        except:
            pass
        time.sleep(1.5)
        show_quiz_question(chat_id, quiz['message_id'])
    else:
        finish_quiz(chat_id, quiz['message_id'])
    bot.answer_callback_query(call.id)


def finish_quiz(chat_id, message_id):
    if chat_id not in active_quizzes:
        return
    quiz = active_quizzes[chat_id]
    db.log_quiz_result(quiz['user_id'], quiz['type'], quiz['score'], len(quiz['questions']))
    total = len(quiz['questions']) * 10
    percent = (quiz['score'] / total * 100) if total > 0 else 0
    text = f"🎮 <b>Викторина завершена!</b>\n\n"
    text += f"🏆 <b>Итоговый счет:</b> {quiz['score']}/{total} очков\n"
    text += f"📊 <b>Результат:</b> {percent:.0f}%\n\n"
    if percent >= 90:
        text += "🎸 <b>Отлично! Вы настоящий музыкальный эксперт!</b>"
    elif percent >= 70:
        text += "🎵 <b>Очень хорошо! Отличные знания музыки!</b>"
    elif percent >= 50:
        text += "🎧 <b>Хорошо! Продолжайте изучать музыку!</b>"
    else:
        text += "🎶 <b>Попробуйте еще раз! Вы обязательно улучшите результат!</b>"
    markup = types.InlineKeyboardMarkup(row_width=2)
    if 'artist_id' in quiz:
        markup.add(
            types.InlineKeyboardButton("🎮 Еще викторина", callback_data=f"quiz_{quiz['artist_id']}"),
            types.InlineKeyboardButton("📖 Об артисте", callback_data=f"artist_{quiz['artist_id']}")
        )
    else:
        markup.add(
            types.InlineKeyboardButton("🎮 Новая викторина", callback_data="quiz_type_random"),
            types.InlineKeyboardButton("📊 Статистика", callback_data="quiz_stats")
        )
    markup.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
    try:
        bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
    except:
        bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
    del active_quizzes[chat_id]


# ---- Запуск бота ----
if __name__ == "__main__":
    print("=" * 60)
    print("🎶 БОТ 'ГОЛОС ВРЕМЕНИ' - ОБНОВЛЕННАЯ ВЕРСИЯ С ОТЧЁТАМИ")
    print("=" * 60)
    print(f"✅ Отчёты будут отправляться администратору (ID: {ADMIN_ID})")
    print("✅ База данных инициализирована")
    print(f"✅ Загружено {len(db.get_all_genres())} жанров")
    print("✅ Исправлена ошибка с отображением статистики и таблицы лидеров")
    print("✅ Все функции работают корректно")
    print("=" * 60)
    print("\nОжидание сообщений...")
    try:
        bot.infinity_polling(timeout=60)
    except Exception as e:
        print(f"Ошибка при запуске бота: {e}")
        print("Перезапуск через 5 секунд...")
        time.sleep(5)