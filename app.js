const my_utils = require('./my_utils.js');
var redis = require("redis");
var Worker = require("./worker.js").Worker;
var client = redis.createClient();
const channel_name = "test_channel";

var worker = null;

var alphabet_generate = my_utils.alphabet_generate;
var random_text = my_utils.random_text;
var getTime = my_utils.getTime;

// Числовые настроечные константы
const generatorAliveDelay = 7000; // если столько времени ничего не было от генератора, то считать его мёртвым
const generatorAliveTestDelay = 2000; // как часто проверять, живой ли генератор
const generatorMessageTimeout = 5000; // частота генерации сообщений
const workersAliveTimeout = 10000; // частота проверки воркеров на живучесть
const workersAliveDelay = 30000; // если столько времени ничего не было от воркера, то считать его мёртвым

var alphabet = alphabet_generate();
var global_last_message_time = null;
var message_id = null;  // todo: изменить на какие-то уникально-генерируемые ID или убрать
var timer_test_workers = null;  // Таймер проверки работоспособности воркеров

var mode = 'worker'; // 'generator' || 'worker'


/*
* Pub/Sub - только для информирования о наличии нового сообщения
* Все мессаджи лежат в одном списке мессаджей.
* Формат мессаджа:
*   text:
*   generate_time:
*
* Имееются следующие списки мессаджей:
*   waiting_messages:
*   in_work_list_${worker.wid}:
*   error_messages:
*
* Также есть специальный массив всех воркеров: workers_alive
*   хранятся просто id воркеров
*   также каждый воркер кладёт по своему ключу: wid: <last_start_time>
*   если этот last_start_time > workersAliveDelay - значит воркер не обработал последнее сообщение, перезалить сообщение из его очереди в общую
*
* Порядок работы worker'a
*   - получить сообщение
*   - положить в свой собственный список in_work_message
*   - прежде чем начать обрабатывать, положить wid: <last_start_time>
*       - если успел положить, то обработать
*           - если ошибка, то положить в специальный массив error_messages
*           - закончив обработку удалить сообщение из своего спика in_work_message
* */


function test_mode() {
    if (mode === 'worker') {
        // смена режима работы если давно не пушились сообщения
        if (global_last_message_time === null || getTime() - global_last_message_time > generatorAliveDelay) {
            // перезапросить через redis last_add_message_time, вдруг кто-то уже успел стать генератором
            client.get('last_add_message_time',
                (err, res) => {
                    global_last_message_time = res && parseInt(res) || 0;
                    if (getTime() - global_last_message_time > generatorAliveDelay) {
                        mode = 'generator';
                        if (worker) worker.stop();
                        console.log('Change mode: ', mode);
                    }
                    test_mode();
                });
            return;
        }
        else {
            setTimeout(test_mode, generatorAliveTestDelay);
            if (!worker) {

                // subscriber.subscribe(channel_name);
                worker = new Worker(channel_name, client, global_last_message_time);
                worker.start();
                console.log('Worker mode started: ', worker.wid);
            }
            else {
                global_last_message_time = worker.last_time_message;
            }
        }
    }

    if (mode === 'generator') {
        if (message_id === null) {
            client.get('message_id',
                (err, res) => {
                    message_id = res && parseInt(res) || 0;
                    test_mode();
                });
            return;
        }

        message_id++;
        let current_time = getTime();
        let message = {
            id: message_id, // можно без этого поля, просто так удобнее было отлаживать
            text: random_text(alphabet),
            generate_time: current_time
        };

        client.multi()
            .lpush("waiting_messages", JSON.stringify(message))
            .set("message_id", message_id.toString())
            .set("last_add_message_time", current_time.toString())
            .exec(function (err, repl) {
                if (err) console.log('errr: ', err);
            });

        client.publish(channel_name, 'add');
        client.llen("waiting_messages", (err, res) => {
            // console.log('Count messages in queue: ', res)
        });
        // console.log('Message Added!', current_time);
        setTimeout(test_mode, generatorMessageTimeout);


        if (!timer_test_workers) {
            timer_test_workers = setInterval(function () {
                client.smembers("workers_alive", (err, replies)=> {
                    // console.log("workers: ", replies);
                    if (err || !replies) return;

                    replies.map((wid) => {
                        client.get(Worker.key_last_time_work_gen(wid), (err, ltw) => {
                            if (err) return; // todo: обработать отдельно
                            // todo: переделать. какому-то воркеру может просто не доставаться рабочих пакетов,
                            // может воркер будет на каждое приходящее сообщение что-то писать, подтверждая что он рабочий
                            if (ltw && getTime() - ltw > workersAliveDelay) { // считать нерабочим, удалить из спика
                                // Удаляем ключ и значение из workers_alive + переносим сообщение из спика обратно
                                client.del(Worker.key_last_time_work_gen(wid));
                                client.multi()
                                    .srem("workers_alive", wid)
                                    .del(Worker.key_last_time_work_gen(wid))
                                    .rpoplpush(Worker.process_queue_name_gen(wid), "waiting_messages")
                                    .exec((err, repl) => {if (err) console.log('errr: ', err);});
                                console.log('Worker deleted!', wid);
                            }
                        });
                    });
                });
            }, workersAliveTimeout);
        }
    }
}

// Обработчик ошибок Redis
client.on("error", (err) => {console.log("Error client: " + err);});

var command_line_args = {};

var args = process.argv.slice(2);
args.forEach(function (elem) {
    var aa = elem.split('=');
    var a = aa[0].replace('--', "");
    var b = aa.length > 1 ? aa[1] : null;
    if (b)
        command_line_args[a] = b;
    else
        command_line_args[a] = true;
});

if (command_line_args.getErrors) {
    client.lrange("error_messages", 0, -1, (err, replies) => {
        if (err || !replies) {console.log('Error: ', err); client.quit(); return; }
        replies.forEach(v => console.log(v));
        console.log('Error Messages: ', replies.length);
        client.ltrim("error_messages", 1, 0, (err, res) => {client.quit();});
    });
    return
}


test_mode();
