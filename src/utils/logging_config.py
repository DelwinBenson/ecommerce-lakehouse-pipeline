import logging

def get_logger(name:str)->logging.Logger:
    logger=logging.getLogger(name) #get a logger with our required name

    if not logger.handlers:
        handler=logging.StreamHandler() #add a handler, this one sends message in console
        formatter=logging.Formatter(
            "%(asctime)s| %(levelname)s| %(message)s"
        ) #logging format
        handler.setFormatter(formatter) #handover format to the handler
        logger.addHandler(handler) #connect handler to the logger

    logger.setLevel(logging.INFO) #set level of logger to get INFO or anything above that

    return logger