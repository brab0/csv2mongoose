var reader;

module.exports.load = function(options, cb){
    const lineReader = require('line-reader');
    reader = {};
    
    lineReader.open(options.file, {
        encoding : options.encoding
    }, function(err, lr) {
        if (err) throw err;
        
        reader = new Reader(Object.assign(options, {reader : lr}));
    });

    return { read: fakeReader }
}

function fakeReader(cb){    
    setTimeout(() => {
        reader ? reader.read(cb) : fakeReader(cb)
    }, 50);
}

class Reader{

    constructor(options){
        this.header = options.header;        
        this.onFinish = options.onFinish;        
        this.reader = options.reader;
        this.separator = options.separator;
        this.index = 0;
        this.mapLinker = options.mapLinker;
        this.indexesMap = this.getIndexesByMapper();
        options.onStart();
    }

    getIndexesByMapper(){
        let map = {};
        const models = require('mongoose').models;
        
        for(let model in models){            
            const schema = models[model].schema.obj;
            const modelKey = model[0].toLowerCase() + model.slice(1);

            map[modelKey] = {};

            for(let prop in schema){
                for(let attr in schema[prop]){
                    if(attr == this.mapLinker.attr){
                        if(this.mapLinker.type == "index"){
                            map[modelKey][prop] = schema[prop][attr];
                        } else if(this.mapLinker.type == "name"){
                            let columns = [];

                            if(!this.columns){
                                const separator = this.separator;
                                let columns = [];

                                this.reader.nextLine((err, line) => columns = line.split(separator));
                                
                                this.columns = columns;
                            }

                            var colIndex = this.columns.map(col => {
                                if(this.mapLinker.model && this.mapLinker.model.remove){                                    
                                    col = col.split(this.mapLinker.model && this.mapLinker.model.separator ? this.mapLinker.model.separator : "").join("");
                                }
                                
                                return col.toLowerCase();
                            }).indexOf(modelKey.toLowerCase() + this.mapLinker.model && this.mapLinker.model.separator ? this.mapLinker.model.separator : "" + prop);

                            if(colIndex > -1){                                
                                map[modelKey][prop] = colIndex;
                            }
                        }                        
                    }
                }
            }  
        }
        
        return map;
    }

    read(cb){
        if (this.reader.hasNextLine()) {            
            return new Promise((resolve, reject) => {
                this.reader.nextLine(function(err, line) {
                    resolve(line)
                });
            })
            .then(line => {
                if(line){                    
                    let cols = line.toString().split(this.separator);
                    let entities = {};
                    
                    // o header do this.mapLinker.type === "name" é obrigatório e lido em getIndexesByMapper()
                    if(this.header || this.mapLinker.type === "name"){
                        for(let entity in this.indexesMap){
                            entities[entity] = this.parse(cols, this.indexesMap[entity]);
                        }                    

                        return cb(entities, ++this.index);
                    }
                
                    this.header = true;
                }
            })
            .then(() => this.read(cb));                
        } else {
            this.onFinish(this.index);

            this.reader.close(function(err) {
                if (err) throw err;
            });
        }   
    }

    parse(cols, map){
        let myObj = {};
        
        for(let index in map){            
            myObj[index] = cols[map[index]];
        }        
        
        return myObj;
    }
}