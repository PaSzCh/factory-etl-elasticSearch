from rx import create
from rx import operators as ops


def kafka_observable(consumer):
    def _observable(observer, _):
        try:
            for msg in consumer:
                observer.on_next(msg.value)
        except Exception as e:
            observer.on_error(e)
    return create(_observable)

def machine_codes():
    return {
        "UNS56A": "UNSCRAMBLER",
        "WS964F": "WASHER",
        "IS8710": "INSPECTION",
        "FB713A": "FILLING",
        "C7841R": "CARBONATOR",
        "CPM784": "CAPPING",
        "LBL74F": "LABELING",
        "PLL741": "PALLETIZER"
    }

def properties_code():
    return{
        "A7": "LITERS", #: Litros utilizados
        "W8": "QUALITY",#: Calidad del resultado
        "L1": "LIGHT",  #: Luz que pasa a través del cristal
        "T3": "TIME",   #: Tiempo
        "P6": "POWER",  #: Fuerza usada
        "G8": "GRADES"  #: Grados
    }

def atributes_code():
    return{
        "TS": "TIMESTAMP",
        "MC": "MACHINE",
        "PR": "PRODUCT",
        "PS": "PROPS"
    }

def machines_mapping(code):
    return machine_codes()[code]

def properties_mapping(code):
    return properties_code()[code]

def attributes_mapping(code):
    return atributes_code()[code]

def auxa(event):
    print(f"a: {event}")

def auxb(event):
    print(f"b: {event}")

def aux(e): 
    #podemos definir la función aquí en lugar de usar una lambda en la siguiente función
    e["MACHINE"] = machines_mapping(e["MACHINE"])
    return e
    #print(e["MACHINE"])
    #pass

def aux2(e):
    e["PROPS"] = {properties_mapping(k):v for k,v in e["PROPS"].items()}
    return e
       
def build_pipeline(source, send_rich_event, save_raw_event, save_rich_event):
    return source.pipe(
        ops.do_action(lambda e: save_raw_event(e)),
        ops.do_action(lambda e: print(f"event received: {e}")),
        ops.filter(lambda e: e["MC"] in machine_codes().keys()),
        ops.map(lambda e: {attributes_mapping(k):e[k] for k in e.keys()}),
        ops.map(aux),
        ops.map(lambda e: {**e, }),
        ops.map(aux2),
        ops.do_action(lambda e: print(f"event received: {e}")),
        ops.do_action(send_rich_event),
        ops.do_action(lambda e: save_rich_event(e)),
        ops.do_action(lambda _: print("rich event sent"))
    )