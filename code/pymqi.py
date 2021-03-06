# Python MQI Class Wrappers. High level classes that for the MQI
# Extension. These present an object interface to MQI.
#
# Author: L. Smithson (lsmithson@open-networks.co.uk)
# Author: Dariusz Suchojad (dsuch at gefira.pl)
#
# DISCLAIMER
# You are free to use this code in any way you like, subject to the
# Python & IBM disclaimers & copyrights. I make no representations
# about the suitability of this software for any purpose. It is
# provided "AS-IS" without warranty of any kind, either express or
# implied. So there.
#
"""
PyMQI - Python MQI Wrapper Classes

These classes wrap the pymqe low level MQI calls. They present an OO
interface with a passing resemblance MQI C++.

Classes are also provided for easy use of the MQI structure parameters
(MQMD, MQGMO etc.) from Python. These allow Python scripts to set/get
structure members by attribute, dictionary etc.

The classes are:

    * MQOpts - Base class for defining MQI parameter structures.
    * CD - MQI MQCD structure class
    * CMHO - MQI MQCMHO structure class
    * MD - MQI MQMD structure class
    * GMO - MQI MQGMO structure class.
    * IMPO - MQI MQIMPO structure class
    * OD - MQI MQOD structure class.
    * PD - MQI MQPD structure class.
    * PMO - MQI MQPMO structure class.
    * RFH2 - MQI MQRFH2 structure class.
    * SCO - MQI MQSCO structure class
    * SMPO - MQI MQSMPO structure class
    * SRO - MQI MQSRO structure class
    * SD - MQI MQSD structure class
    * TM - MQI MQTM structure class
    * TMC2- MQI MQTMC2 structure class
    * Filter/StringFilter/IntegerFilter - PCF/MQAI filters
    * QueueManager - Queue Manager operations
    * Queue - Queue operations
    * Topic - Publish/subscribe topic operations
    * Subscription - Publish/subscribe subscription operations
    * PCFExecute - Programmable Command Format operations
    * Error - Base class for pymqi errors.
    * MQMIError - MQI specific error
    * PYIFError - Pymqi error

The following MQI operations are supported:

    * MQCONN, MQDISC (QueueManager.connect()/QueueManager.disconnect())
    * MQCONNX (QueueManager.connectWithOptions())
    * MQOPEN/MQCLOSE (Queue.open(), Queue.close(), Topic.open(), Topic.close())
    * MQPUT/MQPUT1/MQGET (Queue.put(), QueueManager.put1(), Queue.get())
    * MQCMIT/MQBACK (QueueManager.commit()/QueueManager.backout())
    * MQBEGIN (QueueuManager.begin())
    * MQINQ (QueueManager.inquire(), Queue.inquire())
    * MQSET (Queue.set())
    * MQSUB (Subscription.sub())
    * And various MQAI PCF commands.

The supported command levels (from 5.0 onwards) for the version of MQI
linked with this module are available in the tuple pymqi.__mqlevels__.
For a client build, pymqi.__mqbuild__ is set to the string 'client',
otherwise it is set to 'server'.

To use this package, connect to the Queue Manager (using
QueueManager.connect()), then open a queue (using Queue.open()). You
may then put or get messages on the queue (using Queue.put(),
Queue.get()), as required.

Where possible, pymqi assumes the MQI defaults for all parameters.

Like MQI C++, pymqi can defer a queue connect until the put/get call.

Pymqi maps all MQI warning & error status to the MQMIError
exception. Errors detected by pymqi itself raise the PYIFError
exception. Both these exceptions are subclasses of the Error class.

MQI constants are defined in the CMQC module. PCF constants are
defined in CMQCFC.

PCF commands and inquiries are executed by calling a MQCMD_* method on
an instance of a PCFExecute object.

Pymqi is thread safe. Pymqi objects have the same thread scope as
their MQI counterparts.

"""

# Stdlib
import struct
import exceptions
import types
import threading
import ctypes
# import xml parser.  lxml/etree only available since python 2.5
use_minidom = False
try:
    import lxml.etree
except Exception:
    from xml.dom.minidom import parseString
    use_minidom = True

# PyMQI
import pymqe, CMQC, CMQCFC, CMQXC

__version__ = "1.3"
__mqlevels__ = pymqe.__mqlevels__
__mqbuild__ = pymqe.__mqbuild__


#
# 64bit suppport courtesy of Brent S. Elmer, Ph.D. (mailto:webe3vt@aim.com)
#
# On 64 bit machines when MQ is compiled 64bit, MQLONG is an int defined
# in /opt/mqm/inc/cmqc.h or wherever your MQ installs to.
#
# On 32 bit machines, MQLONG is a long and many other MQ data types are set to MQLONG
#
# So, set MQLONG_TYPE to 'i' for 64bit MQ and 'l' for 32bit MQ so that the
# conversion from the Python data types to C data types in the MQ structures
# will work correctly.
#

# Are we running 64 bit?
if struct.calcsize("P") == 8:
    MQLONG_TYPE = 'i' # 64 bit
else:
    MQLONG_TYPE = 'l' # 32 bit

#######################################################################
#

# MQI Python<->C Structure mapping. MQI uses lots of parameter passing
# structures in its API. These classes are used to set/get the
# parameters in the style of Python dictionaries & keywords. Pack &
# unpack calls are used to convert between python class attributes and
# 'C' structures, suitable for passing in/out of C functions.
#
# The MQOpts class defines common operations for structure definition,
# default values setting, member set/get and translation to & from 'C'
# structures. Specializations construct MQOpts with a list specifying
# structure member names, their default values, and pack/unpack
# formats. MQOpts uses this list to setup class attributes
# corresponding to the structure names, set up attribute defaults, and
# builds a format string usable by the struct package to translate to
# 'C' structures.
#
#######################################################################


class MQOpts:
    """Base class for packing/unpacking MQI Option structures. It is
    constructed with a list defining the member/attribute name,
    default value (from the CMQC module) and the member pack format
    (see the struct module for the formats). The list format is:

      [['Member0', CMQC.DEFAULT_VAL0, 'fmt1']
       ['Member1', CMQC.DEFAULT_VAL1, 'fmt2']
         ...
      ]

    MQOpts defines common methods to allow structure members to be
    set/get as attributes (foo.Member0 = 42), set/get as dictionary
    items (foo['Member0'] = 42) or set as keywords (foo.set(Member0 =
    42, Member1 = 'flipperhat'). The ctor can be passed an optional
    keyword list to initialize the structure members to non-default
    values. The get methods returns all attributes as a dictionary.

    The pack() method packs all members into a 'C' structure according
    to the format specifiers passed to the ctor. The packing order is
    as specified in the list passed to the ctor. Pack returns a string
    buffer, which can be passed directly to the MQI 'C' calls.

    The unpack() method does the opposite of pack. It unpacks a string
    buffer into an MQOpts instance.

    Applications are not expected to use MQOpts directly. Instead,
    MQOpts is sub-classed as particular MQI structures."""

    def __init__(self, list, **kw):
        """MQOpts(memberList [,**kw])

        Initialise the option structure. 'list' is a list of structure
    member names, default values and pack/unpack formats. 'kw' is an
    optional keyword dictionary that may be used to override default
    values set by MQOpts sub-classes."""

        self.__list = list[:]
        self.__format = ''
        # Creat the structure members as instance attributes and build
        # the struct.pack/unpack format string. The attribute name is
        # identical to the 'C' structure member name.
        for i in list:
            setattr(self, i[0], i[1])
            self.__format = self.__format + i[2]
        apply(MQOpts.set, (self,), kw)

    def pack(self):
        """ pack()

        Pack the attributes into a 'C' structure to be passed to MQI
        calls. The pack order is as defined to the MQOpts
        ctor. Returns the structure as a string buffer"""

        # Build tuple for struct.pack() argument. Start with format
        # string.
        args = [self.__format]
        # Now add the current attribute values to the tuple
        for i in self.__list:
            v = getattr(self, i[0])
            # Flatten attribs that are arrays
            if type(v) is types.ListType:
                for x in v:
                    args.append(x)
            else:
                args.append(v)
        return apply(struct.pack, args)

    def unpack(self, buff):
        """unpack(buff)

        Unpack a 'C' structure 'buff' into self."""

        # Unpack returns a tuple of the unpacked data, in the same
        # order (I hope!) as in the ctor's list arg.
        r = struct.unpack(self.__format, buff)
        x = 0
        for i in self.__list:
            setattr(self, i[0], r[x])
            x = x + 1

    def set(self, **kw):
        """set(**kw)

        Set a structure member using the keyword dictionary 'kw'. An
        AttributeError exception is raised for invalid member
        names."""

        for i in kw.keys():
            # Only set if the attribute already exists. getattr raises
            # an exception if it doesn't.
            getattr(self, str(i))
            setattr(self, str(i), kw[i])

    def __setitem__(self, key, value):
        """__setitem__(key, value)

        Set the structure member attribute 'key' to 'value', as in
        obj['Flop'] = 42.
        """

        # Only set if the attribute already exists. getattr raises an
        # exception if it doesn't.
        getattr(self, key)
        setattr(self, key, value)

    def get(self):
        """get()

        Return a dictionary of the current structure member
        values. The dictionary is keyed by a 'C' member name."""

        d = {}
        for i in self.__list:
            d[i[0]] = getattr(self, i[0])
        return d

    def __getitem__(self, key):
        """__getitem__(key)

        Return the member value associated with key, as in print
        obj['Flop']."""
        return getattr(self, key)

    def __str__(self):
        """__str__()

        Pretty Print Structure."""

        rv = ''
        for i in self.__list:
            rv = rv + str(i[0]) + ': ' + str(getattr(self, i[0])) + '\n'
        # Chop the trailing newline
        return rv[:-1]

    def __repr__(self):
        """__repr__()

        Return the packed buffer as a printable string."""
        return str(self.pack())

    def get_length(self):
        """get_length()

        Returns the length of the (would be) packed buffer.

        """

        return struct.calcsize(self.__format)

    def set_vs(self, vs_name, vs_value=None, vs_offset=0, vs_buffer_size=0,
               vs_ccsid=0):
        """set_vs(vs_name, vs_value, vs_offset, vs_buffer_size, vs_ccsid)

        This method aids in the setting of the MQCHARV (variable length
        string) types in MQ structures.  The type contains a pointer to a
        variable length string.  A common example of a MQCHARV type
        is the ObjectString in the MQOD structure.
        In pymqi the ObjectString is defined as 5 separate
        elements (as per MQCHARV):
        ObjectStringVSPtr - Pointer
        ObjectStringVSOffset - Long
        ObjectStringVSBufSize - Long
        ObjectStringVSLength - Long
        ObjectStringVSCCSID - Long

        """

        vs_name_vsptr = ""
        #if the VSPtr name is passed - remove VSPtr to be left with name.
        if vs_name.endswith("VSPtr"):
            vs_name_vsptr = vs_name
        else:
            vs_name_vsptr = vs_name + "VSPtr"

        vs_name_vsoffset = vs_name + "VSOffset"
        vs_name_vsbuffsize = vs_name + "VSBufSize"
        vs_name_vslength = vs_name + "VSLength"
        vs_name_vsccsid = vs_name + "VSCCSID"

        c_vs_value = None
        c_vs_value_p = 0

        if vs_value is not None:
            c_vs_value = ctypes.create_string_buffer(vs_value)
            c_vs_value_p = ctypes.cast(c_vs_value, ctypes.c_void_p).value

        self[vs_name_vsptr] = c_vs_value_p
        self[vs_name_vsoffset] = vs_offset
        self[vs_name_vsbuffsize] = vs_buffer_size
        self[vs_name_vslength] = len(vs_value)
        self[vs_name_vsccsid] = vs_ccsid

    def get_vs(self, vs_name):
        """get_vs(vs_name)

        This method returns the string to which the VSPtr pointer points to.

        """

        vs_name_vsptr = ""
        #if the VSPtr name is passed - remove VSPtr to be left with name.
        if vs_name.endswith("VSPtr"):
            vs_name_vsptr = vs_name
        else:
            vs_name_vsptr = vs_name + "VSPtr"

        c_vs_value = None
        c_vs_value_p = self[vs_name_vsptr]
        if c_vs_value_p != 0:
            c_vs_value = ctypes.cast(c_vs_value_p, ctypes.c_char_p).value

        return c_vs_value


#
# Sub-classes of MQOpts representing real MQI structures.
#

class gmo(MQOpts):
    """gmo(**kw)

    Construct a MQGMO Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments
    'kw'."""

    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQGMO_STRUC_ID, '4s'],
            ['Version', CMQC.MQGMO_VERSION_1, MQLONG_TYPE],
            ['Options', CMQC.MQGMO_NO_WAIT, MQLONG_TYPE],
            ['WaitInterval', 0, MQLONG_TYPE],
            ['Signal1', 0, MQLONG_TYPE],
            ['Signal2', 0, MQLONG_TYPE],
            ['ResolvedQName', '', '48s'],
            ['MatchOptions', CMQC.MQMO_MATCH_MSG_ID+CMQC.MQMO_MATCH_CORREL_ID, MQLONG_TYPE],
            ['GroupStatus', CMQC.MQGS_NOT_IN_GROUP, 'b'],
            ['SegmentStatus', CMQC.MQSS_NOT_A_SEGMENT, 'b'],
            ['Segmentation', CMQC.MQSEG_INHIBITED, 'b'],
            ['Reserved1', ' ', 'c'],
            ['MsgToken', '', '16s'],
            ['ReturnedLength', CMQC.MQRL_UNDEFINED, MQLONG_TYPE],]

        if "7.0" in pymqe.__mqlevels__:
            opts += [
                ['Reserved2', 0L, MQLONG_TYPE],
                ['MsgHandle', 0L, 'q']]

        apply(MQOpts.__init__, (self, tuple(opts)), kw)

# Backward compatibility
GMO = gmo


class pmo(MQOpts):
    """pmo(**kw)

    Construct a MQPMO Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments
    'kw'."""

    def __init__(self, **kw):
        opts = [
            ['StrucId', CMQC.MQPMO_STRUC_ID, '4s'],
            ['Version', CMQC.MQPMO_VERSION_1, MQLONG_TYPE],
            ['Options', CMQC.MQPMO_NONE, MQLONG_TYPE],
            ['Timeout', -1, MQLONG_TYPE],
            ['Context', 0, MQLONG_TYPE],
            ['KnownDestCount', 0, MQLONG_TYPE],
            ['UnknownDestCount', 0, MQLONG_TYPE],
            ['InvalidDestCount', 0, MQLONG_TYPE],
            ['ResolvedQName', '', '48s'],
            ['ResolvedQMgrName', '', '48s'],
            ['RecsPresent', 0, MQLONG_TYPE],
            ['PutMsgRecFields',  0, MQLONG_TYPE],
            ['PutMsgRecOffset', 0, MQLONG_TYPE],
            ['ResponseRecOffset', 0, MQLONG_TYPE],
            ['PutMsgRecPtr', 0, 'P'],
            ['ResponseRecPtr', 0, 'P']]

        if "7.0" in pymqe.__mqlevels__:
            opts += [
                ['OriginalMsgHandle', 0L, 'q'],
                ['NewMsgHandle', 0L, 'q'],
                ['Action', 0L, MQLONG_TYPE],
                ['PubLevel', 0L, MQLONG_TYPE]]

        apply(MQOpts.__init__, (self, tuple(opts)), kw)

# Backward compatibility
PMO = pmo

class od(MQOpts):
    """od(**kw)

    Construct a MQOD Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments
    'kw'."""

    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQOD_STRUC_ID, '4s'],
            ['Version', CMQC.MQOD_VERSION_1, MQLONG_TYPE],
            ['ObjectType', CMQC.MQOT_Q, MQLONG_TYPE],
            ['ObjectName', '', '48s'],
            ['ObjectQMgrName', '', '48s'],
            ['DynamicQName', 'AMQ.*', '48s'],
            ['AlternateUserId', '', '12s'],
            ['RecsPresent', 0, MQLONG_TYPE],
            ['KnownDestCount', 0, MQLONG_TYPE],
            ['UnknownDestCount', 0, MQLONG_TYPE],
            ['InvalidDestCount', 0, MQLONG_TYPE],
            ['ObjectRecOffset', 0, MQLONG_TYPE],
            ['ResponseRecOffset', 0, MQLONG_TYPE],
            ['ObjectRecPtr', 0, 'P'],
            ['ResponseRecPtr', 0, 'P'],
            ['AlternateSecurityId', '', '40s'],
            ['ResolvedQName', '', '48s'],
            ['ResolvedQMgrName', '', '48s'],]

        if "7.0" in pymqe.__mqlevels__:
            opts += [

                # ObjectString
                ['ObjectStringVSPtr', 0, 'P'],
                ['ObjectStringVSOffset', 0L, MQLONG_TYPE],
                ['ObjectStringVSBufSize', 0L, MQLONG_TYPE],
                ['ObjectStringVSLength', 0L, MQLONG_TYPE],
                ['ObjectStringVSCCSID', 0L, MQLONG_TYPE],

                # SelectionString
                ['SelectionStringVSPtr', 0, 'P'],
                ['SelectionStringVSOffset', 0L, MQLONG_TYPE],
                ['SelectionStringVSBufSize', 0L, MQLONG_TYPE],
                ['SelectionStringVSLength', 0L, MQLONG_TYPE],
                ['SelectionStringVSCCSID', 0L, MQLONG_TYPE],

                # ResObjectString
                ['ResObjectStringVSPtr', 0, 'P'],
                ['ResObjectStringVSOffset', 0L, MQLONG_TYPE],
                ['ResObjectStringVSBufSize', 0L, MQLONG_TYPE],
                ['ResObjectStringVSLength', 0L, MQLONG_TYPE],
                ['ResObjectStringVSCCSID', 0L, MQLONG_TYPE],

                ['ResolvedType', -3L, MQLONG_TYPE]]

            # For 64bit platforms MQLONG is an int and this pad
            # needs to be here for WMQ 7.0
            if MQLONG_TYPE == 'i':
                opts += [['pad','', '4s']]

        apply(MQOpts.__init__, (self, tuple(opts)), kw)

# Backward compatibility
OD = od

class md(MQOpts):
    """md(**kw)

    Construct a MQMD Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments
    'kw'."""

    def __init__(self, **kw):
        apply(MQOpts.__init__, (self, (
            ['StrucId', CMQC.MQMD_STRUC_ID, '4s'],
            ['Version', CMQC.MQMD_VERSION_1, MQLONG_TYPE],
            ['Report', CMQC.MQRO_NONE, MQLONG_TYPE],
            ['MsgType', CMQC.MQMT_DATAGRAM, MQLONG_TYPE],
            ['Expiry', CMQC.MQEI_UNLIMITED, MQLONG_TYPE],
            ['Feedback', CMQC.MQFB_NONE, MQLONG_TYPE],
            ['Encoding', CMQC.MQENC_NATIVE, MQLONG_TYPE],
            ['CodedCharSetId', CMQC.MQCCSI_Q_MGR, MQLONG_TYPE],
            ['Format', '', '8s'],
            ['Priority', CMQC.MQPRI_PRIORITY_AS_Q_DEF, MQLONG_TYPE],
            ['Persistence', CMQC.MQPER_PERSISTENCE_AS_Q_DEF, MQLONG_TYPE],
            ['MsgId', '', '24s'],
            ['CorrelId', '', '24s'],
            ['BackoutCount', 0, MQLONG_TYPE],
            ['ReplyToQ', '', '48s'],
            ['ReplyToQMgr', '', '48s'],
            ['UserIdentifier', '', '12s'],
            ['AccountingToken', '', '32s'],
            ['ApplIdentityData', '', '32s'],
            ['PutApplType', CMQC.MQAT_NO_CONTEXT, MQLONG_TYPE],
            ['PutApplName', '', '28s'],
            ['PutDate', '', '8s'],
            ['PutTime', '', '8s'],
            ['ApplOriginData', '', '4s'],
            ['GroupId', '', '24s'],
            ['MsgSeqNumber', 1, MQLONG_TYPE],
            ['Offset', 0, MQLONG_TYPE],
            ['MsgFlags', CMQC.MQMF_NONE, MQLONG_TYPE],
            ['OriginalLength', CMQC.MQOL_UNDEFINED, MQLONG_TYPE])), kw)

# Backward compatibility
MD = md

# RFH2 Header parsing/creation Support - Hannes Wagener - 2010.
class RFH2(MQOpts):
    """RFH2(**kw)

    Construct a RFH2 Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments
    'kw'.  Attempt to parse the RFH2 when unpack is called.

    """

    initial_opts = [['StrucId', CMQC.MQRFH_STRUC_ID, '4s'],
            ['Version', CMQC.MQRFH_VERSION_2, MQLONG_TYPE],
            ['StrucLength', 0, MQLONG_TYPE],
            ['Encoding', CMQC.MQENC_NATIVE, MQLONG_TYPE],
            ['CodedCharSetId', CMQC.MQCCSI_Q_MGR, MQLONG_TYPE],
            ['Format', CMQC.MQFMT_NONE, '8s'],
            ['Flags', 0, MQLONG_TYPE],
            ['NameValueCCSID', CMQC.MQCCSI_Q_MGR, MQLONG_TYPE]]

    big_endian_encodings = [CMQC.MQENC_INTEGER_NORMAL,
                            CMQC.MQENC_DECIMAL_NORMAL,
                            CMQC.MQENC_FLOAT_IEEE_NORMAL,
                            CMQC.MQENC_FLOAT_S390,
                            #17
                            CMQC.MQENC_INTEGER_NORMAL +
                            CMQC.MQENC_DECIMAL_NORMAL,
                            #257
                            CMQC.MQENC_INTEGER_NORMAL +
                            CMQC.MQENC_FLOAT_IEEE_NORMAL,
                            #272
                            CMQC.MQENC_DECIMAL_NORMAL +
                            CMQC.MQENC_FLOAT_IEEE_NORMAL,
                            #273
                            CMQC.MQENC_INTEGER_NORMAL +
                            CMQC.MQENC_DECIMAL_NORMAL +
                            CMQC.MQENC_FLOAT_IEEE_NORMAL]

    def __init__(self, **kw):
        #take a copy of private initial_opts
        self.opts = [list(x) for x in self.initial_opts]
        apply(MQOpts.__init__, (self, tuple(self.opts)), kw)

    def add_folder(self, folder_data):
        """add_folder(folder_data)

        Adds a new XML folder to the RFH2 header.
        Checks if the XML is well formed and updates self.StrucLength.

        """

        folder_name = None
        #check that the folder is valid xml and get the root tag name.
        if use_minidom:
            try:
                folder_name = parseString(folder_data). \
                                            documentElement.tagName
            except Exception, e:
                raise PYIFError("RFH2 - XML Folder not well formed. " \
                                "Exception: %s" % str(e))
        else:
            try:
                folder_name = lxml.etree.fromstring(folder_data).tag
            except Exception, e:
                raise PYIFError("RFH2 - XML Folder not well formed. " \
                                "Exception: %s" % str(e))
        #make sure folder length divides by 4 - else add spaces
        folder_length = len(folder_data)
        remainder = folder_length % 4
        num_spaces = 0
        if remainder != 0:
            num_spaces = 4 - remainder
            folder_data = folder_data + " " * num_spaces
            folder_length = len(folder_data)

        self.opts.append([folder_name + "Length", long(folder_length),
                          MQLONG_TYPE])
        self.opts.append([folder_name, folder_data, "%is" % folder_length])
        #save the current values
        saved_values = self.get()
        #reinit MQOpts with new fields added
        apply(MQOpts.__init__, (self, tuple(self.opts)), )
        #reset the values to the saved values
        self.set(**saved_values)
        #calculate the correct StrucLength
        self["StrucLength"] = self.get_length()

    def pack(self, encoding=None):
        """pack(encoding)

        Override pack in order to set correct numeric encoding in the format.

        """

        if encoding is not None:
            if encoding in self.big_endian_encodings:
                self.opts[0][2] = ">" + self.initial_opts[0][2]
                saved_values = self.get()
                #apply the new opts
                apply(MQOpts.__init__, (self, tuple(self.opts)), )
                #set from saved values
                self.set(**saved_values)

        return MQOpts.pack(self)

    def unpack(self, buff, encoding=None):
        """unpack(buff, encoding)

        Override unpack in order to extract and parse RFH2 folders.
        Encoding meant to come from the MQMD.

        """

        if buff[0:4] != CMQC.MQRFH_STRUC_ID:
            raise PYIFError("RFH2 - StrucId not MQRFH_STRUC_ID. Value: %s" %
                            str(buff[0:4]))

        if len(buff) < 36:
            raise PYIFError("RFH2 - Buffer too short. Should be 36 bytes or " \
                            "longer.  Buffer Length: %s" % str(len(buff)))
        #take a copy of initial_opts and the lists inside
        self.opts = [list(x) for x in self.initial_opts]

        big_endian = False
        if encoding is not None:
            if encoding in self.big_endian_encodings:
                big_endian = True
        else:
            #if small endian first byte of version should be > 'x\00'
            if buff[4:5] == "\x00":
                big_endian = True
        #indicate bigendian in format
        if big_endian:
            self.opts[0][2] = ">" + self.opts[0][2]
        #apply and parse the default header
        apply(MQOpts.__init__, (self, tuple(self.opts)), )
        MQOpts.unpack(self, buff[0:36])

        if self['StrucLength'] < 0:
            raise PYIFError("RFH2 - 'StrucLength' is negative. " \
                            "Check numeric encoding.")

        if len(buff) > 36:
            if self['StrucLength'] > len(buff):
                raise PYIFError("RFH2 - Buffer too short. Expected: " \
                                "%s Buffer Length: %s" %
                                (self['StrucLength'], len(buff)))

        #extract only the string containing the xml folders and loop
        s =  buff[36:self['StrucLength']]
        while s:
            #first 4 bytes is the folder length. supposed to divide by 4.
            len_bytes = s[0:4]
            folder_length = 0
            if big_endian:
                folder_length = struct.unpack(">l", len_bytes)[0]
            else:
                folder_length = struct.unpack("l", len_bytes)[0]

            #move on past four byte length
            s = s[4:]
            #extract the folder string
            folder_data = s[:folder_length]
            #check that the folder is valid xml and get the root tag name
            folder_name = None
            #check that the folder is valid xml and get the root tag name.
            if use_minidom:
                try:
                    folder_name = parseString(folder_data). \
                                                documentElement.tagName
                except Exception, e:
                    raise PYIFError("RFH2 - XML Folder not well formed. " \
                                    "Exception: %s" % str(e))
            else:
                try:
                    folder_name = lxml.etree.fromstring(folder_data).tag
                except Exception, e:
                    raise PYIFError("RFH2 - XML Folder not well formed. " \
                                    "Exception: %s" % str(e))
            #append folder length and folder string to self.opts types
            self.opts.append([folder_name + "Length", long(folder_length),
                              MQLONG_TYPE])
            self.opts.append([folder_name, folder_data, "%is" %
                              folder_length])
            #move on past the folder
            s = s[folder_length:]

        #save the current values
        saved_values = self.get()
        #apply the new opts
        apply(MQOpts.__init__, (self, tuple(self.opts)), )
        #set from saved values
        self.set(**saved_values)
        #unpack the buffer? - should get same result?
        #MQOpts.unpack(self, buff[0:self["StrucLength"]])

class TM(MQOpts):
    """TM(**kw)

    Construct a MQTM Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments 'kw'.

    """

    def __init__(self, **kw):
        apply(MQOpts.__init__, (self, (
            ['StrucId', CMQC.MQTM_STRUC_ID, '4s'],
            ['Version', CMQC.MQTM_VERSION_1, MQLONG_TYPE],
            ['QName', '', '48s'],
            ['ProcessName', '', '48s'],
            ['TriggerData', '', '64s'],
            ['ApplType', 0, MQLONG_TYPE],
            ['ApplId', '', '256s'],
            ['EnvData', '', '128s'],
            ['UserData', '', '128s'])), kw)


class TMC2(MQOpts):
    """TMC2(**kw)

    Construct a MQTMC2 Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments 'kw'.

    """

    def __init__(self, **kw):
        apply(MQOpts.__init__, (self, (
            ['StrucId', CMQC.MQTMC_STRUC_ID, '4s'],
            ['Version', CMQC.MQTMC_VERSION_2, '4s'],
            ['QName', '', '48s'],
            ['ProcessName', '', '48s'],
            ['TriggerData', '', '64s'],
            ['ApplType', '', '4s'],
            ['ApplId', '', '256s'],
            ['EnvData', '', '128s'],
            ['UserData', '', '128s'],
            ['QMgrName', '', '48s'])), kw)

# MQCONNX code courtesy of John OSullivan (mailto:jos@onebox.com)
# SSL additions courtesy of Brian Vicente (mailto:sailbv@netscape.net)

class cd(MQOpts):
    """cd(**kw)

    Construct a MQCD Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments
    'kw'."""

    # The MQCD_VERSION & MQCD_LENGTH_* we're going to use depend on the WMQ
    # version we had been compiled with but it is not known on Python side
    # until runtime so set it once here when first importing pymqi
    # (originally written by Brent S. Elmer, Ph.D. (mailto:webe3vt@aim.com)).

    if '7.5' in pymqe.__mqlevels__:
        _mqcd_version = CMQXC.MQCD_VERSION_10
        _mqcd_current_length = CMQXC.MQCD_LENGTH_10

    elif '7.0' in pymqe.__mqlevels__:
        _mqcd_version = CMQXC.MQCD_VERSION_9
        _mqcd_current_length = CMQXC.MQCD_LENGTH_9

    elif '6.0' in pymqe.__mqlevels__:
        _mqcd_version = CMQXC.MQCD_VERSION_8
        _mqcd_current_length = CMQXC.MQCD_LENGTH_8

    elif '5.3' in pymqe.__mqlevels__:
        _mqcd_version = CMQXC.MQCD_VERSION_7
        _mqcd_current_length = CMQXC.MQCD_LENGTH_7

    else:
        # The default version in MQCD_DEFAULT in cmqxc.h is MQCD_VERSION_6
        _mqcd_version = CMQXC.MQCD_VERSION_6
        _mqcd_current_length = CMQXC.MQCD_LENGTH_6

    def __init__(self, **kw):
        opts = []
        opts += [
            ['ChannelName', '', '20s'],
            ['Version', self._mqcd_version, MQLONG_TYPE],
            ['ChannelType', CMQC.MQCHT_SENDER, MQLONG_TYPE],
            ['TransportType', CMQC.MQXPT_LU62, MQLONG_TYPE],
            ['Desc', '', '64s'],
            ['QMgrName', '', '48s'],
            ['XmitQName', '', '48s'],
            ['ShortConnectionName', '', '20s'],
            ['MCAName', '', '20s'],
            ['ModeName', '', '8s'],
            ['TpName', '', '64s'],
            ['BatchSize', 50L, MQLONG_TYPE],
            ['DiscInterval', 6000L, MQLONG_TYPE],
            ['ShortRetryCount', 10L, MQLONG_TYPE],
            ['ShortRetryInterval', 60L, MQLONG_TYPE],
            ['LongRetryCount', 999999999L, MQLONG_TYPE],
            ['LongRetryInterval', 1200L, MQLONG_TYPE],
            ['SecurityExit', '', '128s'],
            ['MsgExit', '', '128s'],
            ['SendExit', '', '128s'],
            ['ReceiveExit', '', '128s'],
            ['SeqNumberWrap', 999999999L, MQLONG_TYPE],
            ['MaxMsgLength', 4194304L, MQLONG_TYPE],
            ['PutAuthority', CMQC.MQPA_DEFAULT, MQLONG_TYPE],
            ['DataConversion', CMQC.MQCDC_NO_SENDER_CONVERSION, MQLONG_TYPE],
            ['SecurityUserData', '', '32s'],
            ['MsgUserData', '', '32s'],
            ['SendUserData', '', '32s'],
            ['ReceiveUserData', '', '32s'],
            ['UserIdentifier', '', '12s'],
            ['Password', '', '12s'],
            ['MCAUserIdentifier', '', '12s'],
            ['MCAType', CMQC.MQMCAT_PROCESS, MQLONG_TYPE],
            ['ConnectionName', '', '264s'],
            ['RemoteUserIdentifier', '', '12s'],
            ['RemotePassword', '', '12s'],
            ['MsgRetryExit', '', '128s'],
            ['MsgRetryUserData', '', '32s'],
            ['MsgRetryCount', 10L, MQLONG_TYPE],
            ['MsgRetryInterval', 1000L, MQLONG_TYPE],
            ['HeartbeatInterval', 300L, MQLONG_TYPE],
            ['BatchInterval', 0L, MQLONG_TYPE],
            ['NonPersistentMsgSpeed', CMQC.MQNPMS_FAST, MQLONG_TYPE],
            ['StrucLength', self._mqcd_current_length, MQLONG_TYPE],
            ['ExitNameLength', CMQC.MQ_EXIT_NAME_LENGTH, MQLONG_TYPE],
            ['ExitDataLength', CMQC.MQ_EXIT_DATA_LENGTH, MQLONG_TYPE],
            ['MsgExitsDefined', 0L, MQLONG_TYPE],
            ['SendExitsDefined', 0L, MQLONG_TYPE],
            ['ReceiveExitsDefined', 0L, MQLONG_TYPE],
            ['MsgExitPtr', 0, 'P'],
            ['MsgUserDataPtr', 0, 'P'],
            ['SendExitPtr', 0, 'P'],
            ['SendUserDataPtr', 0, 'P'],
            ['ReceiveExitPtr', 0, 'P'],
            ['ReceiveUserDataPtr', 0, 'P'],
            ['ClusterPtr', 0, 'P'],
            ['ClustersDefined', 0L, MQLONG_TYPE],
            ['NetworkPriority', 0L, MQLONG_TYPE],
            ['LongMCAUserIdLength', 0L, MQLONG_TYPE],
            ['LongRemoteUserIdLength', 0L, MQLONG_TYPE],
            ['LongMCAUserIdPtr', 0, 'P'],
            ['LongRemoteUserIdPtr', 0, 'P'],
            ['MCASecurityId', '', '40s'],
            ['RemoteSecurityId', '', '40s']]

        # If SSL is supported, append the options. SSL support is
        # implied by 5.3.
        if "5.3" in pymqe.__mqlevels__:
            opts += [['SSLCipherSpec','','32s'],
                     ['SSLPeerNamePtr',0,'P'],
                     ['SSLPeerNameLength',0L,MQLONG_TYPE],
                     ['SSLClientAuth',0L,MQLONG_TYPE],
                     ['KeepAliveInterval',-1,MQLONG_TYPE],
                     ['LocalAddress','','48s'],
                     ['BatchHeartbeat',0L,MQLONG_TYPE]]
        else:
            # No mqaiExecute means no 5.3, so redefine the struct version
            opts[1] = ['Version', CMQC.MQCD_VERSION_6, MQLONG_TYPE]

        if "6.0" in pymqe.__mqlevels__:
            opts += [['HdrCompList', [0L, -1L], '2' + MQLONG_TYPE],
                     ['MsgCompList', [0] + 15 * [-1L], '16' + MQLONG_TYPE],
                     ['CLWLChannelRank', 0L, MQLONG_TYPE],
                     ['CLWLChannelPriority', 0L, MQLONG_TYPE],
                     ['CLWLChannelWeight', 50L, MQLONG_TYPE],
                     ['ChannelMonitoring', 0L, MQLONG_TYPE],
                     ['ChannelStatistics', 0L, MQLONG_TYPE]]

        if "7.0" in pymqe.__mqlevels__:
            opts += [['SharingConversations', 10, MQLONG_TYPE],
                     ['PropertyControl', 0, MQLONG_TYPE],      # 0 = MQPROP_COMPATIBILITY
                     ['MaxInstances', 999999999, MQLONG_TYPE],
                     ['MaxInstancesPerClient', 999999999, MQLONG_TYPE],
                     ['ClientChannelWeight', 0, MQLONG_TYPE],
                     ['ConnectionAffinity', 1, MQLONG_TYPE]]  # 1 = MQCAFTY_PREFERRED
            
        if '7.1' in pymqe.__mqlevels__:
            opts += [['BatchDataLimit', 5000, MQLONG_TYPE],
                     ['UseDLQ', 2, MQLONG_TYPE],
                     ['DefReconnect', 0, MQLONG_TYPE]]

        # In theory, the pad should've been placed right before the 'MsgExitPtr'
        # attribute, however setting it there makes no effect and that's why
        # it's being set here, as a last element in the list.
        if '7.1' not in pymqe.__mqlevels__:
            if MQLONG_TYPE == 'i':
                opts += [['pad','', '4s']]

        apply(MQOpts.__init__, (self, tuple(opts)), kw)

# Backward compatibility
CD = cd

# SCO Class for SSL Support courtesy of Brian Vicente (mailto:sailbv@netscape.net)
class sco(MQOpts):
    """sco(**kw)

    Construct a MQSCO Structure with default values as per MQI. The
    default values maybe overridden by the optional keyword arguments
    'kw'. """

    def __init__(self, **kw):
        opts = [
            ['StrucId', CMQC.MQSCO_STRUC_ID, '4s'],
            ['Version', CMQC.MQSCO_VERSION_1, MQLONG_TYPE],
            ['KeyRepository', '', '256s'],
            ['CryptoHardware', '', '256s'],
            ['AuthInfoRecCount', 0L, MQLONG_TYPE],
            ['AuthInfoRecOffset', 0L, MQLONG_TYPE],
            ['AuthInfoRecPtr', 0, 'P']]

        # Add new SSL fields defined in 6.0 and update version to 2
        if "6.0" in pymqe.__mqlevels__:
            opts += [['KeyResetCount', 0L, MQLONG_TYPE],
                     ['FipsRequired', 0L, MQLONG_TYPE]]

        apply(MQOpts.__init__, (self, tuple(opts)), kw)

# Backward compatibility
SCO = sco

class SD(MQOpts):
    """SD(**kw)

    Construct a MQSD Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments
    'kw'."""

    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQSD_STRUC_ID, '4s'],
                ['Version', CMQC.MQSD_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQSO_NON_DURABLE, MQLONG_TYPE],
                ['ObjectName', '', '48s'],
                ['AlternateUserId', '', '12s'],
                ['AlternateSecurityId', CMQC.MQSID_NONE, '40s'],
                ['SubExpiry', CMQC.MQEI_UNLIMITED, MQLONG_TYPE],

                # ObjectString
                ['ObjectStringVSPtr', 0, 'P'],
                ['ObjectStringVSOffset', 0L, MQLONG_TYPE],
                ['ObjectStringVSBufSize', 0L, MQLONG_TYPE],
                ['ObjectStringVSLength', 0L, MQLONG_TYPE],
                ['ObjectStringVSCCSID', 0L, MQLONG_TYPE],

                #Subname
                ['SubNameVSPtr', 0, 'P'],
                ['SubNameVSOffset', 0L, MQLONG_TYPE],
                ['SubNameVSBufSize', 0L, MQLONG_TYPE],
                ['SubNameVSLength', 0L, MQLONG_TYPE],
                ['SubNameVSCCSID', 0L, MQLONG_TYPE],

                #SubUserData
                ['SubUserDataVSPtr', 0, 'P'],
                ['SubUserDataVSOffset', 0L, MQLONG_TYPE],
                ['SubUserDataVSBufSize', 0L, MQLONG_TYPE],
                ['SubUserDataVSLength', 0L, MQLONG_TYPE],
                ['SubUserDataVSCCSID', 0L, MQLONG_TYPE],

                ['SubCorrelId', CMQC.MQCI_NONE, '24s'],
                ['PubPriority', CMQC.MQPRI_PRIORITY_AS_Q_DEF, MQLONG_TYPE],
                ['PubAccountingToken', CMQC.MQACT_NONE, '32s'],
                ['PubApplIdentityData', '', '32s'],

                #SelectionString
                ['SelectionStringVSPtr', 0, 'P'],
                ['SelectionStringVSOffset', 0L, MQLONG_TYPE],
                ['SelectionStringVSBufSize', 0L, MQLONG_TYPE],
                ['SelectionStringVSLength', 0L, MQLONG_TYPE],
                ['SelectionStringVSCCSID', 0L, MQLONG_TYPE],

                ['SubLevel', 0, MQLONG_TYPE],

                 #SelectionString
                ['ResObjectStringVSPtr', 0, 'P'],
                ['ResObjectStringVSOffset', 0L, MQLONG_TYPE],
                ['ResObjectStringVSBufSize', 0L, MQLONG_TYPE],
                ['ResObjectStringVSLength', 0L, MQLONG_TYPE],
                ['ResObjectStringVSCCSID', 0L, MQLONG_TYPE]]

        apply(MQOpts.__init__, (self, tuple(opts)), kw)


class SRO(MQOpts):
    """SRO(**kw)

    Construct a MQSRO Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments
    'kw'."""

    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQSRO_STRUC_ID, '4s'],
                ['Version', CMQC.MQSRO_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQSRO_FAIL_IF_QUIESCING, MQLONG_TYPE],
                ['NumPubs', 0, MQLONG_TYPE]]

        apply(MQOpts.__init__, (self, tuple(opts)), kw)

class CMHO(MQOpts):
    """CMHO(**kw)

    Construct an MQCMHO Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments
    'kw'."""
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQCMHO_STRUC_ID, '4s'],
                ['Version', CMQC.MQCMHO_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQCMHO_DEFAULT_VALIDATION, MQLONG_TYPE]]

        apply(MQOpts.__init__, (self, tuple(opts)), kw)

class PD(MQOpts):
    """CMHO(**kw)

    Construct an MQPD Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments
    'kw'."""
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQPD_STRUC_ID, '4s'],
                ['Version', CMQC.MQPD_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQPD_NONE, MQLONG_TYPE],
                ['Support', CMQC.MQPD_SUPPORT_OPTIONAL, MQLONG_TYPE],
                ['Context', CMQC.MQPD_NO_CONTEXT, MQLONG_TYPE],
                ['CopyOptions', CMQC.MQCOPY_DEFAULT, MQLONG_TYPE]]

        apply(MQOpts.__init__, (self, tuple(opts)), kw)

class SMPO(MQOpts):
    """SMPO(**kw)

    Construct an MQSMPO Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments
    'kw'."""
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQSMPO_STRUC_ID, '4s'],
                ['Version', CMQC.MQSMPO_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQSMPO_SET_FIRST, MQLONG_TYPE],
                ['ValueEncoding', CMQC.MQENC_NATIVE, MQLONG_TYPE],
                ['ValueCCSID', CMQC.MQCCSI_APPL, MQLONG_TYPE]]

        apply(MQOpts.__init__, (self, tuple(opts)), kw)

class IMPO(MQOpts):
    """IMPO(**kw)

    Construct an MQIMPO Structure with default values as per MQI. The
    default values may be overridden by the optional keyword arguments
    'kw'."""
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQIMPO_STRUC_ID, '4s'],
                ['Version', CMQC.MQIMPO_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQIMPO_INQ_FIRST, MQLONG_TYPE],
                ['RequestedEncoding', CMQC.MQENC_NATIVE, MQLONG_TYPE],
                ['RequestedCCSID', CMQC.MQCCSI_APPL, MQLONG_TYPE],
                ['ReturnedEncoding', CMQC.MQENC_NATIVE, MQLONG_TYPE],
                ['ReturnedCCSID', 0L, MQLONG_TYPE],
                ['Reserved1', 0L, MQLONG_TYPE],

                # ReturnedName
                ['ReturnedNameVSPtr', 0, 'P'],
                ['ReturnedNameVSOffset', 0L, MQLONG_TYPE],
                ['ReturnedNameVSBufSize', 0L, MQLONG_TYPE],
                ['ReturnedNameVSLength', 0L, MQLONG_TYPE],
                ['ReturnedNameVSCCSID', 0L, MQLONG_TYPE],

                ['TypeString', '', '8s']]

        apply(MQOpts.__init__, (self, tuple(opts)), kw)

#
# A utility to convert a MQ constant to its string mnemonic by groping
# a module dictonary
#

class _MQConst2String:

    def __init__(self, module, prefix):
        self.__module = module;
        self.__prefix = prefix
        self.__stringDict = {}
        self.__lock = threading.Lock()

    def __build(self):
        # Lazily build the dictionary of consts vs. their
        # mnemonic strings from the given module dict. Only those
        # attribute that begins with the prefix are considered.
        self.__lock.acquire()
        if len(self.__stringDict) == 0:
            pfxlen = len(self.__prefix)
            for i in self.__module.__dict__.keys():
                if i[0:pfxlen] == self.__prefix:
                    newKey, newVal = self.__module.__dict__[i], i
                    self.__stringDict[newKey] = newVal
        self.__lock.release()

    def __getitem__(self, code):
        self.__build()
        return self.__stringDict[code]

    def has_key(self, key):
        self.__build()
        return self.__stringDict.has_key(key)


#######################################################################
#
# Exception class that encapsulates MQI completion/reason codes.
#
#######################################################################

class Error(exceptions.Exception):
    """Base class for all pymqi exceptions."""
    pass

class MQMIError(Error):
    """Exception class for MQI low level errors."""
    errStringDicts = (_MQConst2String(CMQC, "MQRC_"), _MQConst2String(CMQCFC, "MQRCCF_"),)

    def __init__(self, comp, reason):
        """MQMIError(comp, reason)

        Construct the error object with MQI completion code 'comp' and
        reason code 'reason'."""

        self.comp, self.reason = comp, reason

    def __str__(self):
        """__str__()

        Pretty print the exception object."""

        return 'MQI Error. Comp: %d, Reason %d: %s' % (self.comp, self.reason, self.errorAsString())

    def errorAsString(self):
        """errorAsString()

        Return the exception object MQI warning/failed reason as its
        mnemonic string."""

        if self.comp == CMQC.MQCC_OK:
            return 'OK'
        elif self.comp == CMQC.MQCC_WARNING:
            pfx = 'WARNING: '
        else:
            pfx = 'FAILED: '

        for d in MQMIError.errStringDicts:
            if d.has_key(self.reason):
                return pfx + d[self.reason]
        return pfx + 'WTF? Error code ' + str(self.reason) + ' not defined'

class PYIFError(Error):
    """Exception class for errors generated by pymqi."""
    def __init__(self, e):
        """PYIFError(e)

        Construct the error object with error string 'e'."""
        self.error = e

    def __str__(self):
        """__str__

        Pretty print the exception object."""
        return 'PYMQI Error: ' + str(self.error)


#######################################################################
#
# MQI Verbs
#
#######################################################################

class QueueManager:
    """QueueManager encapsulates the connection to the Queue Manager. By
    default, the Queue Manager is implicitly connected. If required,
    the connection may be deferred until a call to connect().
    """

    def __init__(self, name = ''):
        """QueueManager(name = '')

        Connect to the Queue Manager 'name' (default value ''). If
        'name' is None, don't connect now, but defer the connection
        until connect() is called."""

        self.__handle = None
        self.__name = name
        self.__qmobj = None
        if name != None:
            self.connect(name)

    def __del__(self):
        """__del__()

        Disconnect the Queue Manager, if connected."""

        if self.__handle:
            if self.__qmobj:
                try:
                    pymqe.MQCLOSE(self.__handle, self.__qmobj)
                except:
                    pass
            try:
                self.disconnect()
            except:
                pass

    def connect(self, name):
        """connect(name)

        Connect immediately to the Queue Manager 'name'."""

        print(33333333, name)
        rv = pymqe.MQCONN(name)
        if rv[1]:
            raise MQMIError(rv[1], rv[2])
        self.__handle = rv[0]
        self.__name = name


# MQCONNX code courtesy of John OSullivan (mailto:jos@onebox.com)
# SSL additions courtesy of Brian Vicente (mailto:sailbv@netscape.net)
# Connect options suggested by Jaco Smuts (mailto:JSmuts@clover.co.za)

    def connectWithOptions(self, name, *bwopts, **kw):
        """connectWithOptions(name [, opts=cnoopts][ ,cd=mqcd][ ,sco=mqsco])
           connectWithOptions(name, cd, [sco])

        Connect immediately to the Queue Manager 'name', using the
        optional MQCNO Options opts, the optional MQCD connection
        descriptor cd and the optional MQSCO SSL options sco.

        The second form is defined for backward compatibility with
        older (broken) versions of pymqi. It connects immediately to
        the Queue Manager 'name', using the MQCD connection descriptor
        cd and the optional MQSCO SSL options sco."""

        # Deal with old style args
        bwoptsLen = len(bwopts)
        if bwoptsLen:
            if bwoptsLen > 2:
                raise exceptions.TypeError('Invalid options: %s' % bwopts)
            if bwoptsLen >= 1:
                kw['cd'] = bwopts[0]
            if bwoptsLen == 2:
                kw['sco'] = bwopts[1]

        else:
            # New style args
            for k in kw.keys():
                if k not in ('opts', 'cd', 'sco'):
                    raise exceptions.TypeError('Invalid option: %s' % k)

        options = CMQC.MQCNO_NONE
        ocd = cd()
        if kw.has_key('opts'):
            options = kw['opts']
        if kw.has_key('cd'):
            ocd = kw['cd']
        if kw.has_key('sco'):
            rv = pymqe.MQCONNX(name, options, ocd.pack(), kw['sco'].pack())
        else:
            rv = pymqe.MQCONNX(name, options, ocd.pack())
        if rv[1]:
            raise MQMIError(rv[1], rv[2])
        self.__handle = rv[0]
        self.__name = name

    # Backward compatibility
    connect_with_options = connectWithOptions

    def connectTCPClient(self, name, cd, channelName, connectString):
        """connectTCPClient(name, cd, channelName, connectString)

        Connect immediately to the remote Queue Manager 'name', using
        a TCP Client connection, with channnel 'channelName' and the
        TCP connection string 'connectString'. All other connection
        optons come from 'cd'."""

        cd.ChannelName = channelName
        cd.ConnectionName = connectString
        cd.ChannelType = CMQC.MQCHT_CLNTCONN
        cd.TransportType = CMQC.MQXPT_TCP
        self.connectWithOptions(name, cd)

    # Backward compatibility
    connect_tcp_client = connectTCPClient

    def disconnect(self):
        """disconnect()

        Disconnect from queue manager, if connected."""

        if self.__handle:
            rv = pymqe.MQDISC(self.__handle)
        else:
            raise PYIFError('not connected')

    def getHandle(self):
        """getHandle()

        Get the queue manager handle. The handle is used for other
        pymqi calls."""

        if self.__handle:
            return self.__handle
        else:
            raise PYIFError('not connected')

    # Backward compatibility
    get_handle = getHandle

    def begin(self):
        """begin()

        Begin a new global transaction.
        """

        rv = pymqe.MQBEGIN(self.__handle)
        if rv[0]:
            raise MQMIError(rv[0], rv[1])

    def commit(self):
        """commit()

        Commits any outstanding gets/puts in the current unit of work."""

        rv = pymqe.MQCMIT(self.__handle)
        if rv[0]:
            raise MQMIError(rv[0], rv[1])

    def backout(self):
        """backout()

        Backout any outstanding gets/puts in the current unit of
        work."""

        rv = pymqe.MQBACK(self.__handle)
        if rv[0]:
            raise MQMIError(rv[0], rv[1])

    def put1(self, qDesc, msg, *opts):
        """put1(qDesc, msg [, mDesc, putOpts])

        Put the single message in string buffer 'msg' on the queue
        using the MQI PUT1 call. This encapsulates calls to MQOPEN,
        MQPUT and MQCLOSE. put1 is the optimal way to put a single
        message on a queue.

        qDesc identifies the Queue either by name (if its a string),
        or by MQOD (if its a pymqi.od() instance).

        mDesc is the pymqi.md() MQMD Message Descriptor for the
        message. If it is not passed, or is None, then a default md()
        object is used.

        putOpts is the pymqi.pmo() MQPMO Put Message Options structure
        for the put1 call. If it is not passed, or is None, then a
        default pmo() object is used.

        If mDesc and/or putOpts arguments were supplied, they may be
        updated by the put1 operation."""

        mDesc, putOpts = apply(commonQArgs, opts)
        if putOpts == None:
            putOpts = pmo()

        # Now send the message
        rv = pymqe.MQPUT1(self.__handle, makeQDesc(qDesc).pack(),
                          mDesc.pack(), putOpts.pack(), msg)
        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])
        mDesc.unpack(rv[0])
        putOpts.unpack(rv[1])


    def inquire(self, attribute):
        """inquire(attribute)

        Inquire on queue manager 'attribute'. Returns either the
        integer or string value for the attribute."""

        if self.__qmobj == None:
            # Make an od for the queue manager, open the qmgr & cache result
            qmod = od(ObjectType = CMQC.MQOT_Q_MGR, ObjectQMgrName = self.__name)
            rv = pymqe.MQOPEN(self.__handle, qmod.pack(), CMQC.MQOO_INQUIRE)
            if rv[-2]:
                raise MQMIError(rv[-2], rv[-1])
            self.__qmobj = rv[0]
        rv = pymqe.MQINQ(self.__handle, self.__qmobj, attribute)
        if rv[1]:
            raise MQMIError(rv[-2], rv[-1])
        return rv[0]

    def _is_connected(self):
        """ Try pinging the queue manager in order to see whether the application
        is connected to it. Note that the method is merely a convienece wrapper
        around MQCMD_PING_Q_MGR, in particular, there's still possibility that
        the app will disconnect between checking QueueManager.is_connected
        and the next MQ call.
        """
        pcf = PCFExecute(self)
        try:
            pcf.MQCMD_PING_Q_MGR()
        except Exception, e:
            return False
        else:
            return True

    is_connected = property(_is_connected)


# Some support functions for Queue ops.
def makeQDesc(qDescOrString):
    "Maybe make MQOD from string. Module Private"
    if type(qDescOrString) is types.StringType:
        return od(ObjectName = qDescOrString)
    else:
        return qDescOrString

make_q_desc = makeQDesc

def commonQArgs(*opts):
    "Process args common to put/get/put1. Module Private."
    l = len(opts)
    if l > 2:
        raise exceptions.TypeError, 'Too many args'
    mDesc = None
    pgOpts = None
    if l > 0:
        mDesc = opts[0]
    if l == 2:
        pgOpts = opts[1]
    if mDesc == None:
        mDesc = md()
    return(mDesc, pgOpts)

common_q_args = commonQArgs


class Queue:

    """Queue encapsulates all the Queue I/O operations, including
    open/close and get/put. A QueueManager object must be already
    connected. The Queue may be opened implicitly on construction, or
    the open may be deferred until a call to open(), put() or
    get(). The Queue to open is identified either by a queue name
    string (in which case a default MQOD structure is created using
    that name), or by passing a ready constructed MQOD class."""


    def __realOpen(self):
        "Really open the queue."
        if self.__qDesc == None:
            raise PYIFError, 'The Queue Descriptor has not been set.'
        rv = pymqe.MQOPEN(self.__qMgr.getHandle(),
                          self.__qDesc.pack(), self.__openOpts)
        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])
        self.__qHandle = rv[0]
        self.__qDesc.unpack(rv[1])

    def __init__(self, qMgr, *opts):
        """Queue(qMgr, [qDesc [,openOpts]])

        Associate a Queue instance with the QueueManager object 'qMgr'
        and optionally open the Queue.

        If qDesc is passed, it identifies the Queue either by name (if
        its a string), or by MQOD (if its a pymqi.od() instance). If
        qDesc is not defined, then the Queue is not opened
        immediately, but deferred to a subsequent call to open().

        If openOpts is passed, it specifies queue open options, and
        the queue is opened immediately. If openOpts is not passed,
        the queue open is deferred to a subsequent call to open(),
        put() or get().

        The following table clarifies when the Queue is opened:

           qDesc  openOpts   When opened
             N       N       open()
             Y       N       open() or get() or put()
             Y       Y       Immediately
        """

        self.__qMgr = qMgr
        self.__qHandle = self.__qDesc = self.__openOpts = None
        l = len(opts)
        if l > 2:
            raise exceptions.TypeError, 'Too many args'
        if l > 0:
            self.__qDesc = makeQDesc(opts[0])
        if l == 2:
            self.__openOpts = opts[1]
            self.__realOpen()

    def __del__(self):
        """__del__()

        Close the Queue, if it has been opened."""

        if self.__qHandle:
            try:
                self.close()
            except:
                pass

    def open(self, qDesc, *opts):
        """open(qDesc [,openOpts])

        Open the Queue specified by qDesc. qDesc identifies the Queue
        either by name (if its a string), or by MQOD (if its a
        pymqi.od() instance). If openOpts is passed, it defines the
        Queue open options, and the Queue is opened immediately. If
        openOpts is not passed, the Queue open is deferred until a
        subsequent put() or get() call."""

        l = len(opts)
        if l > 1:
            raise exceptions.TypeError, 'Too many args'
        if self.__qHandle:
            raise PYIFError('The Queue is already open')
        self.__qDesc = makeQDesc(qDesc)
        if l == 1:
            self.__openOpts = opts[0]
            self.__realOpen()


    def put(self, msg, *opts):
        """put(msg[, mDesc ,putOpts])

        Put the string buffer 'msg' on the queue. If the queue is not
        already open, it is opened now with the option 'MQOO_OUTPUT'.

        mDesc is the pymqi.md() MQMD Message Descriptor for the
        message. If it is not passed, or is None, then a default md()
        object is used.

        putOpts is the pymqi.pmo() MQPMO Put Message Options structure
        for the put call. If it is not passed, or is None, then a
        default pmo() object is used.

        If mDesc and/or putOpts arguments were supplied, they may be
        updated by the put operation."""

        mDesc, putOpts = apply(commonQArgs, opts)
        if putOpts == None:
            putOpts = pmo()
        # If queue open was deferred, open it for put now
        if not self.__qHandle:
            self.__openOpts = CMQC.MQOO_OUTPUT
            self.__realOpen()
        # Now send the message
        rv = pymqe.MQPUT(self.__qMgr.getHandle(), self.__qHandle, mDesc.pack(),
                         putOpts.pack(), msg)
        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])
        mDesc.unpack(rv[0])
        putOpts.unpack(rv[1])

    def put_rfh2(self, msg, *opts):
        """put_rfh2(msg[, mDesc ,putOpts, [rfh2_header, ]])

        Put a RFH2 message. opts[2] is a list of RFH2 headers.
        MQMD and RFH2's must be correct.

        """

        rfh2_buff = ""
        if len(opts) >= 3:
            if opts[2] is not None:
                if not isinstance(opts[2], list):
                    raise exceptions.TypeError("Third item of opts should " \
                                               "be a list.")
                encoding = CMQC.MQENC_NATIVE
                if opts[0] is not None:
                    mqmd = opts[0]
                    encoding = mqmd["Encoding"]

                for rfh2_header in opts[2]:
                    if rfh2_header is not None:
                        rfh2_buff = rfh2_buff + rfh2_header.pack(encoding)
                        encoding = rfh2_header["Encoding"]

                msg = rfh2_buff + msg
            self.put(msg, *opts[0:2])
        else:
            self.put(msg, *opts)

    def get(self, maxLength = None, *opts):
        """get([maxLength [, mDesc, getOpts]])

        Return a message from the queue. If the queue is not already
        open, it is opened now with the option 'MQOO_INPUT_AS_Q_DEF'.

        maxLength, if present, specifies the maximum length for the
        message. If the message received exceeds maxLength, then the
        behavior is as defined by MQI and the getOpts argument.

        If maxLength is not specified, or is None, then the entire
        message is returned regardless of its size. This may require
        multiple calls to the underlying MQGET API.

        mDesc is the pymqi.md() MQMD Message Descriptor for receiving
        the message. If it is not passed, or is None, then a default
        md() object is used.

        getOpts is the pymqi.gmo() MQGMO Get Message Options
        structure for the get call. If it is not passed, or is None,
        then a default gmo() object is used.

        If mDesc and/or getOpts arguments were supplied, they may be
        updated by the get operation."""

        mDesc, getOpts = apply(commonQArgs, opts)
        if getOpts == None:
            getOpts = gmo()
        # If queue open was deferred, open it for put now
        if not self.__qHandle:
            self.__openOpts = CMQC.MQOO_INPUT_AS_Q_DEF
            self.__realOpen()

        # Truncated message fix thanks to Maas-Maarten Zeeman
        if maxLength == None:
            length = 4096
        else:
            length = maxLength

        rv = pymqe.MQGET(self.__qMgr.getHandle(), self.__qHandle,
                        mDesc.pack(), getOpts.pack(), length)

        if not rv[-2]:
            # Everything A OK
            mDesc.unpack(rv[1])
            getOpts.unpack(rv[2])
            return rv[0]

        # Some error. If caller supplied buffer, maybe it wasn't big
        # enough, so just return the error/warning.
        # CAVEAT: If message truncated, this exception loses the
        # partially filled buffer.
        if maxLength != None and maxLength >= 0:
            raise MQMIError(rv[-2], rv[-1])

        # Default buffer used, if not truncated, give up
        if rv[-1] != CMQC.MQRC_TRUNCATED_MSG_FAILED:
            raise MQMIError(rv[-2], rv[-1])

        # Message truncated, but we know its size. Do another MQGET
        # to retrieve it from the queue.
        mDesc.unpack(rv[1])  # save the message id
        length = rv[-3]
        rv = pymqe.MQGET(self.__qMgr.getHandle(), self.__qHandle,
                         mDesc.pack(), getOpts.pack(), length)
        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])
        mDesc.unpack(rv[1])
        getOpts.unpack(rv[2])

        return rv[0]

    def get_rfh2(self, max_length=None, *opts):
        """get_rfh2([maxLength [, mDesc, getOpts, [rfh2_header_1, ]]])

        Get a message and attempt to unpack the rfh2 headers.
        opts[2] should be a empty list.
        Unpacking only attempted if Format in previous header is
        CMQC.MQFMT_RF_HEADER_2.

        """

        if len(opts) >= 3:
            if opts[2] is not None:
                if not isinstance(opts[2], list):
                    raise exceptions.TypeError("Third item of opts should " \
                                               "be a list.")

                msg = self.get(max_length, *opts[0:2])
                mqmd = opts[0]
                rfh2_headers = []
                #If format is not CMQC.MQFMT_RF_HEADER_2 then do not parse.
                format = mqmd["Format"]
                while format == CMQC.MQFMT_RF_HEADER_2:
                    rfh2_header = RFH2()
                    rfh2_header.unpack(msg)
                    rfh2_headers.append(rfh2_header)
                    msg = msg[rfh2_header["StrucLength"]:]
                    format = rfh2_header["Format"]
                opts[2].extend(rfh2_headers)
        else:
            msg = self.get(max_length, *opts)

        return msg

    def close(self, options = CMQC.MQCO_NONE):
        """close([options])

        Close a queue, using options."""

        if not self.__qHandle:
            raise PYIFError('not open')
        rv = pymqe.MQCLOSE(self.__qMgr.getHandle(), self.__qHandle, options)
        if rv[0]:
            raise MQMIError(rv[-2], rv[-1])
        self.__qHandle = self.__qDesc = self.__openOpts = None

    def inquire(self, attribute):
        """inquire(attribute)

        Inquire on queue 'attribute'. If the queue is not already
        open, it is opened for Inquire. Returns either the integer or
        string value for the attribute."""

        if not self.__qHandle:
            self.__openOpts = CMQC.MQOO_INQUIRE
            self.__realOpen()
        rv = pymqe.MQINQ(self.__qMgr.getHandle(), self.__qHandle, attribute)
        if rv[1]:
            raise MQMIError(rv[-2], rv[-1])
        return rv[0]

    def set(self, attribute, arg):
        """set(attribute, arg)

        Sets the Queue attribute to arg."""
        if not self.__qHandle:
            self.__openOpts = CMQC.MQOO_SET
            self.__realOpen()
        rv = pymqe.MQSET(self.__qMgr.getHandle(), self.__qHandle, attribute, arg)
        if rv[1]:
            raise MQMIError(rv[-2], rv[-1])

    def set_handle(self, queue_handle):
        """set_handle(queue_handle)

        Sets the queue handle in the case when a handl;e was returned from a
        previous MQ call.

        """

        self.__qHandle = queue_handle

    def get_handle(self):
        """get_handle()

        Get the queue handle.

        """

        return self.__qHandle

#Publish Subscribe support - Hannes Wagener 2011
class Topic:
    """Topic(queue_manager, topic_name, topic_string, topic_desc, open_opts)

    Topic encapsulates all the Topic I/O operations, including
    publish/subscribe.  A QueueManager object must be already
    connected. The Topic may be opened implicitly on construction, or
    the open may be deferred until a call to open(), pub() or
    (The same as for Queue).

    The Topic to open is identified either by a topic name and/or a topic
    string (in which case a default MQOD structure is created using
    those names), or by passing a ready constructed MQOD class.
    Refer to the "Using topic strings" section in the MQ7 Information Center
    for an explanation of how the topic name and topic string is combined
    to identify a particular topic.

    """

    def __real_open(self):
        """real_open()

        Really open the topic.  Only do this in pub()?

        """

        if self.__topic_desc == None:
            raise PYIFError, 'The Topic Descriptor has not been set.'

        rv = pymqe.MQOPEN(self.__queue_manager.getHandle(),
                          self.__topic_desc.pack(), self.__open_opts)
        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])

        self.__topic_handle = rv[0]
        self.__topic_desc.unpack(rv[1])

    def __init__(self, queue_manager, topic_name=None, topic_string=None, topic_desc=None, open_opts=None):
        """Topic(queue_manager, [topic_name, topic_string, topic_desc [,open_opts]])

        Associate a Topic instance with the QueueManager object 'queue_manager'
        and optionally open the Topic.

        If topic_desc is passed ignore topic_string and topic_name.

        If open_opts is passed, it specifies topic open options, and
        the topic is opened immediately. If open_opts is not passed,
        the queue open is deferred to a subsequent call to open(),
        pub().

        The following table clarifies when the Topic is opened:

        topic_desc  open_opts   When opened
             N       N       open()
             Y       N       open() or pub()
             Y       Y       Immediately
        """

        self.__queue_manager = queue_manager
        self.__topic_handle = None
        self.__topic_desc = topic_desc
        self.__open_opts = open_opts

        self.topic_name = topic_name
        self.topic_string = topic_string

        if self.__topic_desc:
            if self.__topic_desc["ObjectType"] is not CMQC.MQOT_TOPIC:
                raise PYIFError, 'The Topic Descriptor ObjectType is not '\
                                 'MQOT_TOPIC.'
            if self.__topic_desc["Version"] is not CMQC.MQOD_VERSION_4:
                raise PYIFError, 'The Topic Descriptor Version is not '\
                                 'MQOD_VERSION_4.'
        else:
            self.__topic_desc = self.__create_topic_desc(topic_name, topic_string)

        if self.__open_opts:
            self.__real_open()

    def __create_topic_desc(self, topic_name, topic_string):
        """__create_topic(topic_name, topic_string)

        Creates a topic object descriptor from a given topic_name/topic_string.

        """

        topic_desc = od()
        topic_desc["ObjectType"] = CMQC.MQOT_TOPIC
        topic_desc["Version"] = CMQC.MQOD_VERSION_4

        if topic_name:
            topic_desc["ObjectName"] = topic_name

        if topic_string:
            topic_desc.set_vs("ObjectString", topic_string, 0, 0, 0)

        return topic_desc

    def __del__(self):
        """__del__()

        Close the Topic, if it has been opened."""
        try:
            if self.__topic_handle:
                self.close()
        except:
            pass

    def open(self, topic_name=None, topic_string=None, topic_desc=None, open_opts=None):
        """open(topic_name, topic_string, topic_desc, open_opts)

        Open the Topic specified by topic_desc or create a object descriptor
        from topic_name and topic_string.
        If open_opts is passed, it defines the
        Topic open options, and the Topic is opened immediately. If
        open_opts is not passed, the Topic open is deferred until a
        subsequent pub() call.

        """

        if self.__topic_handle:
            raise PYIFError('The Topic is already open.')

        if topic_name:
            self.topic_name = topic_name

        if topic_string:
            self.topic_string = topic_string

        if topic_desc:
            self.__topic_desc = topic_desc
        else:
            self.__topic_desc = self.__create_topic_desc(self.topic_name, self.topic_string)

        if open_opts:
            self.__open_opts = open_opts
            self.__real_open()

    def pub(self, msg, *opts):
        """pub(msg[, msg_desc ,put_opts])

        Publish the string buffer 'msg' to the Topic. If the Topic is not
        already open, it is opened now. with the option 'MQOO_OUTPUT'.

        msg_desc is the pymqi.md() MQMD Message Descriptor for the
        message. If it is not passed, or is None, then a default md()
        object is used.

        put_opts is the pymqi.pmo() MQPMO Put Message Options structure
        for the put call. If it is not passed, or is None, then a
        default pmo() object is used.

        If msg_desc and/or put_opts arguments were supplied, they may be
        updated by the put operation.

        """

        msg_desc, put_opts = apply(commonQArgs, opts)

        if put_opts == None:
            put_opts = pmo()

        # If queue open was deferred, open it for put now
        if not self.__topic_handle:
            self.__open_opts = CMQC.MQOO_OUTPUT
            self.__real_open()
        # Now send the message
        rv = pymqe.MQPUT(self.__queue_manager.getHandle(), self.__topic_handle, msg_desc.pack(),
                         put_opts.pack(), msg)
        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])

        msg_desc.unpack(rv[0])
        put_opts.unpack(rv[1])

    def sub(self, *opts):
        """sub(sub_desc, sub_queue)

        Subscribe to the topic and return a Subscription object.
        A subscription to a topic can be made using an existing queue, either
        by pasing a Queue object or a string at which case the queue will
        be opened with default options.

        """

        sub_desc = None
        if len(opts) > 0:
            sub_desc = opts[0]

        sub_queue = None
        if len(opts) > 1:
            sub_queue = opts[1]

        sub = Subscription(self.__queue_manager)
        sub.sub(sub_desc=sub_desc, sub_queue=sub_queue, topic_name=self.topic_name, topic_string=self.topic_string)

        return sub

    def close(self, options = CMQC.MQCO_NONE):
        """close([options])

        Close the topic, using options.

        """

        if not self.__topic_handle:
            raise PYIFError('Topic not open.')

        rv = pymqe.MQCLOSE(self.__queue_manager.getHandle(), self.__topic_handle, options)
        if rv[0]:
            raise MQMIError(rv[-2], rv[-1])

        self.__topic_handle = None
        self.__topic_desc = None
        self.__open_opts = None


class Subscription:
    """Subscription(queue_manager, sub_descm sub_name, sub_queue, topic_name,
                    topic_string)

    Subscription encapsulates a subscription to a topic.

    """
    def __init__(self, queue_manager, sub_desc=None, sub_name=None,
                 sub_queue=None, sub_opts=None, topic_name=None, topic_string=None):
        self.__queue_manager = queue_manager
        self.sub_queue = sub_queue
        self.__sub_desc = sub_desc
        self.sub_name = sub_name
        self.sub_opts = sub_opts
        self.topic_name = topic_name
        self.topic_string = topic_string

        if self.__sub_desc:
            self.sub(sub_desc=self.__sub_desc)

    def get_sub_queue(self):
        """get_sub_queue()

        Return the subscription queue.

        """
        return self.sub_queue


    def get(self, max_length=None, *opts):
        """

        Get a publication from the Queue.

        """

        return self.sub_queue.get(max_length, *opts)

    def sub(self, sub_desc=None, sub_queue=None, sub_name=None, sub_opts=None,
            topic_name=None, topic_string=None):
        """sub(sub_desc, sub_queue)

        Subscribe to a topic, alter or resume a subscription.
        Executes the MQSUB call with parameters.
        The subscription queue can be either passed as a Queue object or a
        Queue object handle.

        """

        if topic_name:
            self.topic_name = topic_name
        if topic_string:
            self.topic_string = topic_string
        if sub_name:
            self.sub_name = sub_name

        if sub_desc:
            if not isinstance(sub_desc, SD):
                raise exceptions.TypeError("sub_desc must be a " \
                                               "SD(sub descriptor) object.")
        else:
            sub_desc = SD()
            if sub_opts:
                sub_desc["Options"] = sub_opts
            else:
                sub_desc["Options"] = CMQC.MQSO_CREATE + \
                                      CMQC.MQSO_NON_DURABLE + \
                                      CMQC.MQSO_MANAGED
            if self.sub_name:
                sub_desc.set_vs("SubName", self.sub_name)
            if self.topic_name:
                sub_desc["ObjectName"] = self.topic_name
            if self.topic_string:
                sub_desc.set_vs("ObjectString", self.topic_string)
        self.__sub_desc = sub_desc

        sub_queue_handle = CMQC.MQHO_NONE
        if sub_queue:
            if isinstance(sub_queue, Queue):
                sub_queue_handle = sub_queue.get_handle()
            else:
                sub_queue_handle = sub_queue

        rv = pymqe.MQSUB(self.__queue_manager.getHandle(), sub_desc.pack(),
                         sub_queue_handle)

        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])

        sub_desc.unpack(rv[0])
        self.__sub_desc = sub_desc
        self.sub_queue = Queue(self.__queue_manager)
        self.sub_queue.set_handle(rv[1])
        self.__sub_handle = rv[2]

    def close(self, sub_close_options=CMQC.MQCO_NONE, close_sub_queue=False, close_sub_queue_options=CMQC.MQCO_NONE):

        if not self.__sub_handle:
            raise PYIFError('Subscription not open.')

        rv = pymqe.MQCLOSE(self.__queue_manager.getHandle(), self.__sub_handle, sub_close_options)
        if rv[0]:
            raise MQMIError(rv[-2], rv[-1])

        self.__sub_handle = None
        self.__sub_desc = None
        self.__open_opts = None

        if close_sub_queue:
            self.sub_queue.close(close_sub_queue_options)

    def __del__(self):
        """__del__()

        Close the Subscription, if it has been opened."""
        try:
            if self.__sub_handle:
                self.close()
        except:
            pass
            
class MessageHandle(object):
    """ A higher-level wrapper around the MQI's native MQCMHO structure.
    """

    # When accessing message properties, this will be the maximum number
    # of characters a value will be able to hold. If it's not enough
    # an exception will be raised and its 'actual_value_length' will be
    # filled in with the information of how many characters there are actually
    # so that an application may re-issue the call.
    default_value_length = 64

    class _Properties(object):
        """ Encapsulates access to message properties.
        """
        def __init__(self, conn_handle, msg_handle):
            self.conn_handle = conn_handle
            self.msg_handle = msg_handle

        def __getitem__(self, name):
            """ Allows for a dict-like access to properties,
            handle.properties[name]
            """
            value = self.get(name)
            if not value:
                raise KeyError('No such property [%s]' % name)

            return value

        def __setitem__(self, name, value):
            """ Implements 'handle.properties[name] = value'.
            """
            return self.set(name, value)

        def get(self, name, default=None, max_value_length=None,
                impo_options=CMQC.MQIMPO_INQ_FIRST, pd=CMQC.MQPD_NONE,
                property_type=CMQC.MQTYPE_AS_SET):
            """ Returns the value of message property 'name'. 'default' is the
            value to return if the property is missing. 'max_value_length'
            is the maximum number of characters the underlying pymqe function
            is allowed to allocate for fetching the value
            (defaults to MessageHandle.default_value_length). 'impo_options'
            and 'pd_options' describe options of MQPD and MQIMPO structures
            to be used and 'property_type' points to the expected data type
            of the property.
            """
            if not max_value_length:
                max_value_length = MessageHandle.default_value_length

            comp_code, comp_reason, value = pymqe.MQINQMP(self.conn_handle,
                    self.msg_handle, impo_options, name, impo_options,
                    property_type, max_value_length)

            if comp_code != CMQC.MQCC_OK:
                raise MQMIError(comp_code, comp_reason)

            return value


        def set(self, name, value, property_type=CMQC.MQTYPE_STRING,
                value_length=CMQC.MQVL_NULL_TERMINATED, pd=None, smpo=None):
            """ Allows for setting arbitrary properties of a message. 'name'
            and 'value' are mandatory. All other parameters are OK to use as-is
            if 'value' is a string. If it isn't a string, the 'property_type'
            and 'value_length' should be set accordingly. For further
            customization, you can also use 'pd' and 'smpo' parameters for
            passing in MQPD and MQSMPO structures.
            """
            pd = pd if pd else PD()
            smpo = smpo if smpo else SMPO()

            comp_code, comp_reason = pymqe.MQSETMP(self.conn_handle,
                    self.msg_handle, smpo.pack(), name, pd.pack(),
                    property_type, value, value_length)

            if comp_code != CMQC.MQCC_OK:
                raise MQMIError(comp_code, comp_reason)

    def __init__(self, qmgr=None, cmho=None):
        self.conn_handle = qmgr.get_handle() if qmgr else CMQC.MQHO_NONE
        cmho = cmho if cmho else CMHO()

        comp_code, comp_reason, self.msg_handle = pymqe.MQCRTMH(self.conn_handle,
                                                            cmho.pack())

        if comp_code != CMQC.MQCC_OK:
            raise MQMIError(comp_code, comp_reason)

        self.properties = self._Properties(self.conn_handle, self.msg_handle)
        
class _Filter(object):
    """ The base class for MQAI filters. The initializer expectes user to provide
    the selector, value and the operator to use. For instance, the can be respectively
    MQCA_Q_DESC, 'MY.QUEUE.*', MQCFOP_LIKE. Compare with the pymqi.Filter class.
    """
    _pymqi_filter_type = None
    
    def __init__(self, selector, value, operator):
        self.selector = selector
        self.value = value
        self.operator = operator
        
    def __repr__(self):
        msg = '<%s at %s %s:%s:%s>'
        return msg % (self.__class__.__name__, hex(id(self)), self.selector, 
                      self.value, self.operator)

class StringFilter(_Filter):
    """ A subclass of pymqi._Filter suitable for passing MQAI string filters around.
    """
    _pymqi_filter_type = 'string'
    
class IntegerFilter(_Filter):
    """ A subclass of pymqi._Filter suitable for passing MQAI integer filters around.
    """
    _pymqi_filter_type = 'integer'
    
class FilterOperator(object):
    """ Creates low-level filters basing on what's been provided in the high-level
    pymqi.Filter object.
    """
    operator_mapping = {
        'less': CMQCFC.MQCFOP_LESS,
        'equal': CMQCFC.MQCFOP_EQUAL,
        'greater': CMQCFC.MQCFOP_GREATER,
        'not_less': CMQCFC.MQCFOP_NOT_LESS,
        'not_equal': CMQCFC.MQCFOP_NOT_EQUAL,
        'not_greater': CMQCFC.MQCFOP_NOT_GREATER,
        'like': CMQCFC.MQCFOP_LIKE,
        'not_like': CMQCFC.MQCFOP_NOT_LIKE,
        'contains': CMQCFC.MQCFOP_CONTAINS,
        'excludes': CMQCFC.MQCFOP_EXCLUDES,
        'contains_gen': CMQCFC.MQCFOP_CONTAINS_GEN,
        'excludes_gen': CMQCFC.MQCFOP_EXCLUDES_GEN,
        }
    
    def __init__(self, pub_filter):
        self.pub_filter = pub_filter
        
    def __call__(self, value):
        
        # Do we support the given attribute filter?
        if self.pub_filter.selector >= CMQC.MQIA_FIRST and self.pub_filter.selector <= CMQC.MQIA_LAST:
            priv_filter_class = IntegerFilter
        elif self.pub_filter.selector >= CMQC.MQCA_FIRST and self.pub_filter.selector <= CMQC.MQCA_LAST:
            priv_filter_class = StringFilter
        else:
            msg = 'selector [%s] is of an unsupported type (neither integer ' \
                'nor a string attribute). Send details to http://packages.' \
                'python.org/pymqi/support-consulting-contact.html'
            raise Error(msg % self.pub_filter.selector)

        # Do we support the operator?
        operator = self.operator_mapping.get(self.pub_filter.operator)
        if not operator:
            msg = 'Operator [%s] is not supported.'
            raise Error(msg % self.pub_filter.operator)

        return priv_filter_class(self.pub_filter.selector, value, operator)
    
class Filter(object):
    """ The user-facing MQAI filtering class which provides syntactic sugar
    on top of pymqi._Filter and its base classes.
    """
    def __init__(self, selector):
        self.selector = selector
        self.operator = None
        
    def __getattribute__(self, name):
        """ A generic method for either fetching the pymqi.Filter object's
        attributes or calling magic methods like 'like', 'contains' etc.
        """
        if name in('selector', 'operator'):
            return object.__getattribute__(self, name)
        self.operator = name
                
        return FilterOperator(self)
#
# This piece of magic shamelessly plagiarised from xmlrpclib.py. It
# works a bit like a C++ STL functor.
#
class _Method:
    def __init__(self, pcf, name):
        self.__pcf = pcf
        self.__name = name

    def __getattr__(self, name):
        return _Method(self.__pcf, "%s.%s" % (self.__name, name))

    def __call__(self, *args):
        if self.__name[0:7] == 'CMQCFC.':
            self.__name = self.__name[7:]
        if self.__pcf.qm:
            qmHandle = self.__pcf.qm.getHandle()
        else:
            qmHandle = self.__pcf.getHandle()
        if len(args):
            rv = pymqe.mqaiExecute(qmHandle, CMQCFC.__dict__[self.__name], *args)
        else:
            rv = pymqe.mqaiExecute(qmHandle, CMQCFC.__dict__[self.__name])
        if rv[1]:
            raise MQMIError(rv[-2], rv[-1])
        return rv[0]

#
# Execute a PCF commmand. Inspired by Maas-Maarten Zeeman
#

class PCFExecute(QueueManager):

    """Send PCF commands or inquiries using the MQAI
    interface. PCFExecute must be connected to the Queue Manager
    (using one of the techniques inherited from QueueManager) before
    its used. PCF commands are executed by calling a CMQCFC defined
    MQCMD_* method on the object.  """

    iaStringDict = _MQConst2String(CMQC, "MQIA_")
    caStringDict = _MQConst2String(CMQC, "MQCA_")

    def __init__(self, name = ''):
        """PCFExecute(name = '')

        Connect to the Queue Manager 'name' (default value '') ready
        for a PCF command. If name is a QueueManager instance, it is
        used for the connection, otherwise a new connection is made """

        if isinstance(name, QueueManager):
            self.qm = name
            QueueManager.__init__(self, None)
        else:
            self.qm = None
            QueueManager.__init__(self, name)

    def __getattr__(self, name):
        """MQCMD_*(attrDict)

        Execute the PCF command or inquiry, passing an an optional
        dictionary of MQ attributes.  The command must begin with
        MQCMD_, and must be one of those defined in the CMQCFC
        module. If attrDict is passed, its keys must be an attribute
        as defined in the CMQC or CMQCFC modules (MQCA_*, MQIA_*,
        MQCACH_* or MQIACH_*). The key value must be an int or string,
        as appropriate.

        If an inquiry is executed, a list of dictionaries (one per
        matching query) is returned. Each dictionary encodes the
        attributes and values of the object queried. The keys are as
        defined in the CMQC module (MQIA_*, MQCA_*), The values are
        strings or ints, as appropriate.

        If a command was executed, or no inquiry results are
        available, an empty listis returned.  """

        return _Method(self, name)

    def stringifyKeys(self, rawDict):
        """stringifyKeys(rawDict)

        Return a copy of rawDict with its keys converted to string
        mnemonics, as defined in CMQC. """

        rv = {}
        for k in rawDict.keys():
            if type(rawDict[k]) is types.StringType:
                d = PCFExecute.caStringDict
            else:
                d = PCFExecute.iaStringDict
            try:
                rv[d[k]] = rawDict[k]
            except KeyError, e:
                rv[k] = rawDict[k]
        return rv

    # Backward compatibility
    stringify_keys = stringifyKeys

class ByteString(object):
    """ A simple wrapper around string values, suitable for passing into PyMQI
    calls wherever IBM's docs state a 'byte string' object should be passed in.
    """
    def __init__(self, value):
        self.value = value
        self.pymqi_byte_string = True

    def __len__(self):
        return len(self.value)
    
def connect(queue_manager, channel=None, conn_info=None):
    """ A convenience wrapper for connecting to MQ queue managers. If given the
    'queue_manager' parameter only, will try connecting to it in bindings mode.
    If given both 'channel' and 'conn_info' will connect in client mode.
    A pymqi.QueueManager is returned on successfully establishing a connection.
    """
    if queue_manager and channel and conn_info:
        qmgr = QueueManager(None)
        qmgr.connect_tcp_client(queue_manager, CD(), channel, conn_info)
        return qmgr
    
    elif queue_manager:
        qmgr = QueueManager(queue_manager)
        return qmgr
