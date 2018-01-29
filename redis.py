import asyncore
import socket
import signal
import time
import re


class Singleton(type):

  _instances = {}
  def __call__(cls, *args, **kwargs):
    if cls not in cls._instances:
      cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
    else:
      cls._instances[cls].__init__(*args, **kwargs)
    return cls._instances[cls]



class RedisData(object):

  __metaclass__ = Singleton



class RedisCommand(object):

  def __init__(self, command, arguments):
    self.datastore = RedisData()
    self.command = command.upper()
    self.arguments = arguments

  @classmethod
  def from_handler(cls, arguments):
    command = arguments.split()[0]
    arguments = arguments.split()[1:]
    return RedisCommand(command, arguments)

  def execute(self):
    return getattr(self, self.command)()

  def QUIT(self):
    return (1, 'OK\n')

  def PING(self):
    if self.arguments:
      return (0, '"{}"\n'.format(" ".join(self.arguments)))
    else:
      return (0, '"PONG"\n'.format())

  def ECHO(self):
    return (0, '"{}"\n'.format(" ".join(self.arguments)))

  def KEYS(self):
    if self.arguments[0] == '*':
      pattern = '.*'
    else:
      pattern = self.arguments[0]
    pattern = re.compile(pattern)
    output = ''
    for index, value in enumerate(vars(self.datastore)):
      if pattern.search(value):
        output += '{}) "{}"\n'.format(index + 1, value)
    return (0, '{}'.format(output))

  def SET(self):
    key = self.arguments[0]
    value = self.arguments[1]
    options = self.arguments[2:]
    mode = None
    expire = None
    for option in options:
      if mode == 'EX':
        expire = int(time.time()) + int(option)
      mode = option
    setattr(self.datastore, key, (value, expire))
    return (0, 'OK\n')

  def GET(self):
    key = self.arguments[0]
    try:
      value, expire = getattr(self.datastore, key)
      if expire is not None and expire <= int(time.time()):
        self.DEL(key)
        raise Exception
      else:
        return (0, '"{}"\n'.format(value))
    except:
      return (0, '(nil)\n')

  def DEL(self, key=None):
    if key is None:
      for key in self.arguments:
        try:
          delattr(self.datastore, key)
        except:
          pass
      return (0, 'OK\n')
    else:
      delattr(self.datastore, key)

  def DBSIZE(self):
    return (0, '(integer) {}\n'.format(len(vars(self.datastore))))

  def INCR(self):
    key = self.arguments[0]
    value, expire = getattr(self.datastore, key, (0, None))
    try:
      value = int(value) + 1
    except:
      return (0, 'Key {} contains a string that can not be represented as integer\n'.format(key))
    setattr(self.datastore, key, (value, expire))
    return (0, '(integer) {}\n'.format(value))

  def ZADD(self):
    def byScore(item):
      return item[0]

    key = self.arguments[0]
    score = self.arguments[1]
    member = self.arguments[2]
    array = getattr(self.datastore, key, [])
    if [value for value in array if value[1] == member]:
      array.remove(value)
    array.append((score, member))
    array = sorted(array, key=byScore)
    setattr(self.datastore, key, array)
    return (0, '(integer) 1\n')

  def ZCARD(self):
    key = self.arguments[0]
    array = getattr(self.datastore, key, [])
    return (0, '(integer) {}\n'.format(len(array)))

  def ZRANK(self):
    key = self.arguments[0]
    member = self.arguments[1]
    array = getattr(self.datastore, key, [])
    if not [position for position, value in enumerate(array) if value[1] == member]:
      return (0, '(nil)\n')
    else:
      return (0, '(integer) {}\n'.format([position for position, value in enumerate(array) if value[1] == member][0]))

  def ZRANGE(self):
    key = self.arguments[0]
    start = int(self.arguments[1])
    stop = int(self.arguments[2])
    array = getattr(self.datastore, key, [])
    if abs(start) > len(array):
      return (0, '(empty list or set)\n')
    if abs(stop) > len(array) or stop == -1:
      stop = None
    output = ''
    for position, value in enumerate([value[1] for value in array[start:stop]]):
      output += '{}) {}\n'.format(position + 1, value)
    return (0, '{}'.format(output))



class RedisHandler(asyncore.dispatcher_with_send):

  def handle_read(self):
    data = self.recv(8192)
    if data:
      command = RedisCommand.from_handler(data)
      code, message = command.execute()
      self.send(message)
      if code:
        self.handle_close()

  def handle_close(self):
    print 'Closing connection from {}:{}'.format(*self.addr)
    self.close()



class RedisServer(asyncore.dispatcher):

  def __init__(self, host, port):
    asyncore.dispatcher.__init__(self)
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
    self.set_reuse_addr()
    self.bind((host, port))
    self.listen(5)
    print 'Server listening at {}:{}'.format(host, port)

  def handle_accept(self):
    pair = self.accept()
    if pair is not None:
      sock, addr = pair
      print 'Opening connection from {}:{}'.format(*addr)
      RedisHandler(sock)

  def handle_close(self):
    print 'Server stopping'
    self.close()



if __name__ == "__main__":

  def signal_handler(signal, frame):
    server.handle_close()

  signal.signal(signal.SIGINT, signal_handler)
  server = RedisServer('localhost', 5555)
  asyncore.loop()
