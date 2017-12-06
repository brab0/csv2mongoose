const lineReader = require('line-reader');

module.exports.load = function(options){
    lineReader.open(options.file, {
        encoding : options.encoding
    }, function(err, reader) {
        if (err) throw err;
        
        return new Reader(Object.assign(options, {reader : reader}));
    });    
}

class Reader{

    constructor(options){
        this.header = options.header;        
        this.onFinish = options.onFinish;        
        this.reader = options.reader;
        this.separator = options.separator;
        this.index = 0;
        this.indexesMap = this.getIndexesMap(options.models, options.mapLinker);
        options.onStart();
    }

    getIndexesMap(models, mapLinker){
        let map = {};
        
        for(let model in models){            
            const schema = models[model].schema.obj;
            const modelKey = model[0].toLowerCase() + model.slice(1);

            map[modelKey] = {};

            for(let prop in schema){    
              for(let attr in schema[prop]){
                if(attr == mapLinker){
                  map[modelKey][prop] = schema[prop][attr];
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
                    let cols = line.split(this.separator);
                    let entities = {};

                    if(this.header){
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
                process.exit(0);
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