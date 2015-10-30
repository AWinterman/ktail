import logging

def get_logger(name=__name__):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s: %(message)s',
                        datefmt='%d.%m.%Y %H:%M:%S')

    return logging.getLogger('ktail.{0}'.format(name))