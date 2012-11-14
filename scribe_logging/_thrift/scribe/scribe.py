#
# Autogenerated by Thrift Compiler (0.8.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py:twisted
#

from thrift.Thrift import TType, TMessageType, TException
from ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None

from zope.interface import Interface, implements
from twisted.internet import defer
from thrift.transport import TTwisted

class Iface(Interface):
  def Log(messages):
    """
    Parameters:
     - messages
    """
    pass


class Client:
  implements(Iface)

  def __init__(self, transport, oprot_factory):
    self._transport = transport
    self._oprot_factory = oprot_factory
    self._seqid = 0
    self._reqs = {}

  def Log(self, messages):
    """
    Parameters:
     - messages
    """
    self._seqid += 1
    d = self._reqs[self._seqid] = defer.Deferred()
    self.send_Log(messages)
    return d

  def send_Log(self, messages):
    oprot = self._oprot_factory.getProtocol(self._transport)
    oprot.writeMessageBegin('Log', TMessageType.CALL, self._seqid)
    args = Log_args()
    args.messages = messages
    args.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def recv_Log(self, iprot, mtype, rseqid):
    d = self._reqs.pop(rseqid)
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      return d.errback(x)
    result = Log_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.success is not None:
      return d.callback(result.success)
    return d.errback(TApplicationException(TApplicationException.MISSING_RESULT, "Log failed: unknown result"))


class Processor(TProcessor):
  implements(Iface)

  def __init__(self, handler):
    self._handler = Iface(handler)
    self._processMap = {}
    self._processMap["Log"] = Processor.process_Log

  def process(self, iprot, oprot):
    (name, type, seqid) = iprot.readMessageBegin()
    if name not in self._processMap:
      iprot.skip(TType.STRUCT)
      iprot.readMessageEnd()
      x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
      oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.trans.flush()
      return defer.succeed(None)
    else:
      return self._processMap[name](self, seqid, iprot, oprot)

  def process_Log(self, seqid, iprot, oprot):
    args = Log_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = Log_result()
    d = defer.maybeDeferred(self._handler.Log, args.messages)
    d.addCallback(self.write_results_success_Log, result, seqid, oprot)
    return d

  def write_results_success_Log(self, success, result, seqid, oprot):
    result.success = success
    oprot.writeMessageBegin("Log", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()


# HELPER FUNCTIONS AND STRUCTURES

class Log_args:
  """
  Attributes:
   - messages
  """

  thrift_spec = (
    None, # 0
    (1, TType.LIST, 'messages', (TType.STRUCT,(LogEntry, LogEntry.thrift_spec)), None, ), # 1
  )

  def __init__(self, messages=None,):
    self.messages = messages

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.LIST:
          self.messages = []
          (_etype3, _size0) = iprot.readListBegin()
          for _i4 in xrange(_size0):
            _elem5 = LogEntry()
            _elem5.read(iprot)
            self.messages.append(_elem5)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('Log_args')
    if self.messages is not None:
      oprot.writeFieldBegin('messages', TType.LIST, 1)
      oprot.writeListBegin(TType.STRUCT, len(self.messages))
      for iter6 in self.messages:
        iter6.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class Log_result:
  """
  Attributes:
   - success
  """

  thrift_spec = (
    (0, TType.I32, 'success', None, None, ), # 0
  )

  def __init__(self, success=None,):
    self.success = success

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.I32:
          self.success = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('Log_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.I32, 0)
      oprot.writeI32(self.success)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
