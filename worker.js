const my_utils = require('./my_utils.js');
var redis = require("redis");

function unique_worker_id() {
    return new Date().getTime() % Math.pow(10, 8) + my_utils.randomInteger(100, 999); // todo: change to 10 ** 8
}

class Worker {
    constructor(channel_name, redis_client, last_time_message) {
        this._id = unique_worker_id();
        this.channel_name = channel_name;
        this.is_started = false;
        this.in_work = false;  // Для имитации сложной обработки сообщения. + 1 воркер может обрабатывать только 1 сообщение
        this.have_new_message = false; // Флаг, определяющий, что в очереди есть новые сообщения, пока обрабатывалось предыдущее.
        this.subscriber = redis.createClient();  // Для получения сообщений от генератора
        this.client = redis_client;  // Для доступа к redis

        this.last_time_message = last_time_message;

        this.subscriber.on("error", (err) => { console.log("Error sub: " + err); });
        this.subscriber.on("message", this.on_message.bind(this));

    }

    get wid() { return this._id; }

    // для того, чтобы генератор мог работать с очередями мёртвых воркеров
    static process_queue_name_gen(id) {
        return `in_work_list_${id}`;
    }

    get process_queue_name() {
        return Worker.process_queue_name_gen(this.wid);
    }

    static key_last_time_work_gen(id) {
        return `worker_ltw_${id}`;
    }

    on_message(channel, message_str) {
        if (!this.is_started) return;
        if (this.channel_name === channel) {
            this.last_time_message = my_utils.getTime();
            // console.log(`New message from ${channel} : ${message_str}`);
            if (!this.in_work)
                this.process();
            else
                this.have_new_message = true;
        }
    }

    process(recursive) {
        // recursive - если в очереди скопились сообщения, то для обработки их всех сразу. Обрабатывать будет до ошибки
        if (!this.is_started || this.in_work) return;
        // переносим это сообщение в свою личную очередь и обрабатываем его
        var self = this;
        this.client.rpoplpush("waiting_messages", this.process_queue_name, function (err, item) {
            if (err || !item) {
                self.have_new_message = false;
                return;
            }
            self.client.set(Worker.key_last_time_work_gen(self.wid), my_utils.getTime());
            self.in_work = true;  // Начало обработки
            console.log('Message start processed: ', JSON.parse(item).id);
            setTimeout(function () {
                if (Math.random() < 0.05) {  // error message
                    self.client.rpush("error_messages", item);
                }
                else {  // Сообщение обработано, удалить из своей очереди
                    self.client.lrem(self.process_queue_name, 1, item);
                }
                console.log('Message processed: ', item);
                self.in_work = false;
                if (recursive || self.have_new_message) self.process(true);

            }, my_utils.randomInteger(1000, 3000));

        })
    }

    start() {
        if (this.is_started) return;
        var self = this;
        // добавить свой айдишник в множество "workers_alive" - чтобы внешний наблюдатель мог перезалить долго выполняемые сообщения
        this.client.sadd("workers_alive", this.wid);
        // подписаться на получение сообщений
        this.subscriber.subscribe(this.channel_name, function () {
            self.is_started = true;
            self.process(true);
        });
        console.log(`Worker<${this.wid}> is started`);
    }

    stop() {
        if (!this.is_started) return;
        this.subscriber.unsubscribe(this.channel_name);
        this.client.multi()
            .srem("workers_alive", this.wid)
            .del(Worker.key_last_time_work_gen(this.wid))
            .exec((err, repl) => {if (err) console.log('errr: ', err);});
        this.subscriber.quit();
        this.is_started = false;
        console.log(`Worker<${this.wid}> stopped`);
    }

}


module.exports = {
    Worker: Worker
};