function randomInteger(min, max) { return Math.floor(min + Math.random() * (max + 1 - min)); }
function randomChoice(arr) { return arr && arr[Math.floor(Math.random() * arr.length)]; }


module.exports = {
    randomInteger: randomInteger,
    randomChoice: randomChoice,
    alphabet_generate: function () {
        let alphabet = [];
        // numbers, A-Z, a-z
        [[48, 57], [65, 91], [97, 122]].map((ll) => {
            for (let i = ll[0]; i <= ll[1]; i++) alphabet.push(String.fromCharCode(i));
        });
        return alphabet;
    },
    random_text: function (alphabet) {
        let text = [];
        let count = randomInteger(5, 40);
        while (count > 0) {
            text.push(randomChoice(alphabet));
            count--;
        }
        return text.join('');
    },
    getTime: function () { return new Date().getTime(); } // todo: заменить на время из redis (client.time() -> начиная с версии 2.6.0)

};