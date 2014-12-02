// Head file that expose interface for code generated dll to link to scopeengine.dll
#pragma once

#include <string>
#include <vector>
#include <exception>
#include <functional>
#include <memory>
#include <iostream>
#include <sstream>
#include <guiddef.h>
#include "StreamCacheClientBasics.h"
using namespace std;

// Windows types used throughout the NativeRuntime
typedef int              BOOL;
typedef long             LONG;
typedef __int64          LONGLONG;
typedef __int64          INT64;
typedef unsigned char    BYTE;
typedef unsigned short   USHORT;
typedef unsigned short   UINT16;
typedef unsigned int     UINT;
typedef unsigned long    ULONG;
typedef unsigned __int64 ULONGLONG;
typedef unsigned __int64 UINT64;
typedef unsigned __int64 SIZE_T;
typedef unsigned long    DWORD;
typedef void *           HANDLE;
typedef void *           PVOID;
typedef void *           LPVOID;
typedef DWORD            LCTYPE;
typedef DWORD            LCID;

#define LOCALE_SSHORTDATE             0x0000001F   // short date format string, eg "MM/dd/yyyy"
#define LOCALE_SLONGDATE              0x00000020   // long date format string, eg "dddd, MMMM dd, yyyy"
#define LOCALE_SSHORTTIME             0x00000079   // Returns the preferred short time format (ie: no seconds, just h:mm)
#define LOCALE_STIMEFORMAT            0x00001003   // time format string, eg "HH:mm:ss"
#define LOCALE_S1159                  0x00000028   // AM designator, eg "AM"
#define LOCALE_S2359                  0x00000029   // PM designator, eg "PM"
#define LOCALE_SMONTHDAY              0x00000078   // Returns the preferred month/day format
#define LOCALE_SYEARMONTH             0x00001006   // year month format string, eg "MM/yyyy"
#define LOCALE_SDAYNAME1              0x0000002A   // long name for Monday
#define LOCALE_SABBREVDAYNAME1        0x00000031   // abbreviated name for Monday
#define LOCALE_SMONTHNAME1            0x00000038   // long name for January
#define LOCALE_SABBREVMONTHNAME1      0x00000044   // abbreviated name for January

#define FALSE 0
#define TRUE  1

typedef LONG CsError;

template<typename T, unsigned int size>
unsigned int array_size(T(&)[size]){return size;}

class CsRandom;

namespace ScopeEngine
{
        /// <summary>
        /// Scope column data types define in ScopeEngine, to seperate from ScopeRuntime::ColumnDataType
        /// </summary>
        enum ColumnType
        {
            /// <summary>
            /// String column
            /// </summary>
            T_String = 0,

            /// <summary>
            /// byte[] column
            /// </summary>
            T_Binary = 1,

            /// <summary>
            /// bool column
            /// </summary>
            T_Boolean = 2,

            /// <summary>
            /// bool? column
            /// </summary>
            T_BooleanQ = 3,

            /// <summary>
            /// int column
            /// </summary>
            T_Integer = 4,

            /// <summary>
            /// int? column
            /// </summary>
            T_IntegerQ = 5,

            /// <summary>
            /// long column
            /// </summary>
            T_Long = 6,

            /// <summary>
            /// long? column
            /// </summary>
            T_LongQ = 7,

            /// <summary>
            /// float column
            /// </summary>
            T_Float = 8,

            /// <summary>
            /// float? column
            /// </summary>
            T_FloatQ = 9,

            /// <summary>
            /// double column
            /// </summary>
            T_Double = 10,

            /// <summary>
            /// double? column
            /// </summary>
            T_DoubleQ = 11,

            /// <summary>
            /// DateTime column
            /// </summary>
            T_DateTime = 12,

            /// <summary>
            /// DateTime? column
            /// </summary>
            T_DateTimeQ = 13,

            /// <summary>
            /// Not a legal column type
            /// </summary>
            T_Error = 14,

            /// <summary>
            /// decimal column
            /// </summary>
            T_Decimal = 15,

            /// <summary>
            /// decimal? column
            /// </summary>
            T_DecimalQ = 16,

            /// <summary>
            /// fake column type representing the null value
            /// </summary>
            T_Null = 17,

            /// <summary>
            /// For user defined type.
            /// </summary>
            T_UDT = 18,

            /// <summary>
            /// This used to be ScopeUrl ... unfortunately the entry cannot be removed as it would cause an off-by-one shift for the types below
            /// </summary>
            T_unused = 19,

            /// <summary>
            /// System.UInt8 type column
            /// </summary>
            T_Byte = 20,

            /// <summary>
            /// System.Int8? type column
            /// </summary>
            T_ByteQ = 21,

            /// <summary>
            /// System.Int8 type column
            /// </summary>
            T_SByte = 22,

            /// <summary>
            /// System.UInt8? type column
            /// </summary>
            T_SByteQ = 23,

            /// <summary>
            /// System.Int16 type column
            /// </summary>
            T_Short = 24,

            /// <summary>
            /// System.Int16? type column
            /// </summary>
            T_ShortQ = 25,

            /// <summary>
            /// System.UInt16 type column
            /// </summary>
            T_UShort = 26,

            /// <summary>
            /// System.UInt16 type column
            /// </summary>
            T_UShortQ = 27,

            /// <summary>
            /// System.UInt32 type column
            /// </summary>
            T_UInt = 28,

            /// <summary>
            /// System.UInt32? type column
            /// </summary>
            T_UIntQ = 29,

            /// <summary>
            /// System.UInt64 type column
            /// </summary>
            T_ULong = 30,

            /// <summary>
            /// System.UInt64? type column
            /// </summary>
            T_ULongQ = 31,

            /// <summary>
            /// System.Guid type column
            /// </summary>
            T_Guid = 32,

            /// <summary>
            /// System.Guid? type column
            /// </summary>
            T_GuidQ = 33, 

            /// <summary>
            /// Minimum type column
            /// </summary>
            T_Minimal = 34,

            /// <summary>
            /// Maximum type column
            /// </summary>
            T_Maximal = 35,

            /// <summary>
            /// Nested STRUCT column
            /// </summary>
            T_Struct = 36,

            /// <summary>
            /// MAP type column
            /// </summary>
            T_Map = 37,

            /// <summary>
            /// ARRAY type column
            /// </summary>
            T_Array = 38,

            /// <summary>
            /// System.Char column
            /// </summary>
            T_Char = 39,

            /// <summary>
            /// System.Char? type column
            /// </summary>
            T_CharQ = 40

            // Should align new type id with ScopeRuntime::ColumnDataType
        };

        enum ScopeCEPMode
        {
            SCOPECEP_MODE_NONE = 0,
            SCOPECEP_MODE_SIMULATE = 1,
            SCOPECEP_MODE_REAL = 2,
        };

        class ScopeDateTime;

        SCOPE_ENGINE_API void LogInitialize();

        //////////////////////
        // Error handling
        //////////////////////

        typedef unsigned int ErrorNumber;

        const ErrorNumber E_USER_ERROR = 0x10000000;
        const ErrorNumber E_USER_OUT_OF_MEMORY = E_USER_ERROR + 1;
        const ErrorNumber E_USER_DECIMAL_ERROR = E_USER_ERROR + 2;
        const ErrorNumber E_USER_EXTRACT_ERROR = E_USER_ERROR + 3;
        const ErrorNumber E_USER_ROW_TOO_BIG = E_USER_ERROR + 4;
        const ErrorNumber E_USER_EXPRESSION_ERROR = E_USER_ERROR + 5;
        const ErrorNumber E_USER_TOO_MANY_RANGES = E_USER_ERROR + 6;
        const ErrorNumber E_USER_INDEX_KEY_TOO_LONG = E_USER_ERROR + 7;
        const ErrorNumber E_USER_KEY_TOO_BIG = E_USER_ERROR + 8;
        const ErrorNumber E_USER_COLUMN_TOO_BIG = E_USER_ERROR + 9;
        const ErrorNumber E_USER_STRING_TOO_BIG = E_USER_ERROR + 10;
        const ErrorNumber E_USER_BINARY_TOO_BIG = E_USER_ERROR + 11;
        const ErrorNumber E_USER_INVALID_CTI = E_USER_ERROR + 12;
        const ErrorNumber E_USER_MISMATCH_ROW = E_USER_ERROR + 13;
        const ErrorNumber E_SYSTEM_ERROR = 0x80000000;
        const ErrorNumber E_COSMOS_STORE_ERROR = E_SYSTEM_ERROR + 1;
        const ErrorNumber E_SYSTEM_METADATA_ERROR = E_SYSTEM_ERROR + 2;
        const ErrorNumber E_SYSTEM_INTERNAL_ERROR = E_SYSTEM_ERROR + 3;
        const ErrorNumber E_SYSTEM_CORRUPT_SS = E_SYSTEM_ERROR + 4;
        const ErrorNumber E_SYSTEM_STREAMCACHE_ERROR = E_SYSTEM_ERROR + 5;
        const ErrorNumber E_SYSTEM_HEAP_OUT_OF_MEMORY = E_SYSTEM_ERROR + 6;
        const ErrorNumber E_SYSTEM_DATA_UNAVAILABLE = E_SYSTEM_ERROR + 7;
        const ErrorNumber E_SYSTEM_RESOURCE_OPEN = E_SYSTEM_ERROR + 8;
        const ErrorNumber E_SYSTEM_RESOURCE_READ = E_SYSTEM_ERROR + 9;
        const ErrorNumber E_SYSTEM_CRT_ERROR = E_SYSTEM_ERROR + 10;


        class SCOPE_ENGINE_API ErrorManager
        {
        public:
            virtual void Reset() = 0;
            virtual void SetError(ErrorNumber errnum, const std::string& param, const std::string &details, const std::string& diagnostics) = 0;
            virtual bool IsError() const = 0;
            virtual bool IsUserError() const = 0;
            virtual std::string ErrorMessageX() const = 0;
    
            static void AssertFailed(const char * expr, const char * func, const char * file, int line);
            static std::string FormatCosmosError(CsError err);
    
            static ErrorManager* GetGlobal();

            virtual ~ErrorManager() {}
        };
    
#define SCOPE_ASSERT(_expr_) { if (!(_expr_)) ErrorManager::AssertFailed(#_expr_, __FUNCTION__, __FILE__, __LINE__); }
    
#pragma warning( push )
#pragma warning( disable : 4251 )
    
        class SCOPE_ENGINE_API ExceptionWithStack : public std::exception
        {
        protected:
            std::string m_details;
            std::string m_stack;
            ErrorNumber m_errorNumber;
        public:
            ExceptionWithStack(ErrorNumber errorNumber, bool captureStack);

            ExceptionWithStack()
            {
            }

            virtual ~ExceptionWithStack()
            {
            }

            ExceptionWithStack(const ExceptionWithStack& other)
            {
                m_details = other.m_details;
                m_stack = other.m_stack;
                m_errorNumber = other.m_errorNumber;
            }

            virtual ExceptionWithStack* Clone() const
            {
                return new ExceptionWithStack(*this);
            }

            const std::string GetDetails() const
            {
                return m_details;
            }

            const std::string& GetStack() const
            {
                return m_stack;
            }

            const ErrorNumber GetErrorNumber() const
            {
                return m_errorNumber;
            }
        };
    
        class SCOPE_ENGINE_API RuntimeException : public ExceptionWithStack
        {
            std::string m_description;
    
        public:
            RuntimeException(ErrorNumber errorNumber, const char * description) :
                ExceptionWithStack(errorNumber, true),
                m_description(description)
            {
            }

            RuntimeException(ErrorNumber errorNumber, const std::string& description) :
                ExceptionWithStack(errorNumber, true),
                m_description(description)
            {
            }

            RuntimeException(const RuntimeException& other): ExceptionWithStack(other)
            {
                m_description = other.m_description;
            }

            virtual ExceptionWithStack* Clone() const
            {
                return new RuntimeException(*this);
            }
    
            virtual const char* what() const
            {
                return m_description.c_str();
            }
        };

        class SCOPE_ENGINE_API ManagedCodeException : public ExceptionWithStack
        {
            std::string m_description;
    
        public:
            ManagedCodeException(const char * description, const char * details) :
                ExceptionWithStack(E_USER_ERROR, false),
                m_description(description)
            {
                m_details = details;
            }

            ManagedCodeException(const ManagedCodeException& other): ExceptionWithStack(other)
            {
                m_description = other.m_description;
            }

            virtual ExceptionWithStack* Clone() const
            {
                return new ManagedCodeException(*this);
            }
    
            virtual const char* what() const
            {
                return m_description.c_str();
            }
        };

#pragma warning( pop )

    //////////////////////
    // Memory Manager section
    //////////////////////

    // A set of buffer tags
    enum BufferTags
    {
        // A general catch-all category
        BT_Glogal,

        // I/O buffers
        BT_IO,

        // Sort buffers
        BT_Sort,

        // Hash agg/join buffers
        BT_Hash,

        // Any buffers used during the execution
        BT_Execution,

        // buffers located externally in the Stream Cache Service
        BT_StreamCache
    };

    // result of retrieving managed memory status
    enum MemoryStatusResult
    {
        // memory status was retrieved successfully
        MSR_Success = 0,

        // memory status request was failed
        MSR_Failure = 1,

        // ScopeHost is not used for vertex execution; get memory status from the call to CG class
        MSR_NoClrHosting = 2,
    };

    using namespace StreamCache;

    // The buffer descriptor
    struct BufferDescriptor
    {
        LPVOID      m_buffer;
        SIZE_T      m_size;

        BufferTags  m_tag;

        LONGLONG    m_created;

        BufferDescriptor() : m_buffer(nullptr), m_size(0), m_tag(BT_Glogal), m_created(0){};
    };

    // The reservation descriptor
    struct ReservationDescriptor : public BufferDescriptor
    {
        SIZE_T  m_commited;
    };

    struct PooledBufferDescriptor : public BufferDescriptor
    {
        ICacheBlock* m_scBlock;

        PooledBufferDescriptor() : BufferDescriptor() { m_scBlock = nullptr;};
    };

    class TreeNode;

    class SCOPE_ENGINE_API MemoryManager
    {
    public:
        static const SIZE_T x_maxMemSize = 2147483648; // 2G (maximum for a row)
        static const SIZE_T x_vertexMemoryLimit = 6442450944; // 6G
        static const SIZE_T x_vertexReserveMemory = 2147483648; // 2G
        static const SIZE_T x_vertexInputVirtualMemory = 4398046510080; // 4T

        virtual const BufferDescriptor* Allocate(__in SIZE_T size, __in BufferTags tag) = 0;
        virtual void Deallocate(__in const BufferDescriptor* desc) = 0;

        virtual const ReservationDescriptor* Reserve(__in SIZE_T size, __in BufferTags tag) = 0;
        virtual void Release(__in const ReservationDescriptor* desc) = 0;

        virtual const BufferDescriptor* Commit(__in const ReservationDescriptor* res, __in SIZE_T size) = 0;
        virtual void Decommit(const BufferDescriptor* desc) = 0;

        virtual void DumpExecutionStats() = 0;

        virtual void WriteRuntimeStats(TreeNode & root) = 0;

        virtual LONG GetNextAllocatorId() = 0;

        static void CalculateIOBufferSize(int inputCnt, int outputCnt, SIZE_T memoryLimit, SIZE_T reservedMemory, SIZE_T & inputBufSize, int & inputBufCnt, SIZE_T & outputBufSize, int & outputBufCnt, SIZE_T & inputVirtualMemLimit);

        virtual MemoryStatusResult GetMemoryLoadStat(unsigned long & loadPercent, unsigned __int64 & availableBytes) = 0;

        static MemoryManager* GetGlobal();

        virtual ~MemoryManager() {}
    };

    class SCOPE_ENGINE_API ExpandableBufferBase
    {
    public:
        static ExpandableBufferBase* Create(__in MemoryManager* memMgr, 
            __in SIZE_T reserveSize, 
            __in SIZE_T size, 
            __in BufferTags tags);
        virtual void Expand(__in SIZE_T size) = 0;
        virtual void Shrink( ) = 0;
        virtual const BufferDescriptor* GetBufferDescriptor() const = 0;
        virtual void* Buffer() const = 0;
        virtual SIZE_T Size() const = 0;
        virtual SIZE_T ReserveSize() const = 0;
        virtual void WriteRuntimeStats(TreeNode & root) = 0;
        virtual ~ExpandableBufferBase() {}
    };

    // A simple stream interface on top of expandable buffer.
    // Defines:
    //  Manipulators:
    //      Put, Write, Flush, Seekp
    //  Accessors:
    //      Tellp, Buffer
    class AutoBuffer
    {
        static const SIZE_T x_blockSize = 64*1024;
        static const SIZE_T x_reserveSize = 256*1024*1024;

        const SIZE_T  m_reserveSize;
        const SIZE_T  m_blockSize;

        std::shared_ptr<ExpandableBufferBase> m_bufferX;

        // alias to m_bufferX->Buffer()
        // doesn't change after construction.
        BYTE* m_buffer;
        
        // next put position in the buffer. (reset by flush)
        SIZE_T	m_position;

        // cache the buffer size. It should be updated whenever bufferX is expanded or shrank. 
        SIZE_T m_bufferSize;

    public:
        AutoBuffer(MemoryManager* memMgr, SIZE_T reserve = x_reserveSize, SIZE_T blockSize = x_blockSize)
            : m_reserveSize (reserve), m_blockSize(blockSize)
        {
            SCOPE_ASSERT(reserve >= blockSize);
            std::shared_ptr<ExpandableBufferBase> ptr(ExpandableBufferBase::Create(memMgr, m_reserveSize, m_blockSize, BT_Execution));
            Init(ptr);
            Reset();
        }

        // The put position since open or last flush.
        SIZE_T Tellp() const
        {
            return m_position;
        }
        
        BYTE* Buffer() const
        {
            return m_buffer;
        }

        SIZE_T Capacity() const
        {
            return m_bufferSize;
        }

        void Seekp(SIZE_T p)
        {
            SCOPE_ASSERT(p <= m_bufferSize);
            m_position = p;
        }

        // put a char
        void Put(BYTE b)
        {
            Reserve(1);
            m_buffer[m_position++] = b;
        }

        // write a block
        void Write(const void* buf, SIZE_T size)
        {
            Reserve(size);
            memcpy( &m_buffer[m_position], buf, size);
            m_position += size;
        }

        // basically discard the stuff in memory,
        // reset the counters
        void Reset()
        {
            m_position = 0;
            m_bufferSize = m_bufferX->Size();
        }

        void Expand(SIZE_T size = x_blockSize)
        {
            Reserve(size);
        }

        void Reserve(SIZE_T size)
        {
            while(m_position + size > m_bufferSize)
            {
                ExpandOneBlock();
            }
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            if (m_bufferX != NULL)
            {
                m_bufferX->WriteRuntimeStats(root);
            }
        }

    private:
        void Init(std::shared_ptr<ExpandableBufferBase>& bufferX)
        {
            m_bufferX = bufferX;
            m_buffer = reinterpret_cast<BYTE*>(bufferX->Buffer());
            m_bufferSize = bufferX->Size();
        }

        void ExpandOneBlock()
        {
            m_bufferX->Expand(m_blockSize);
            m_bufferSize = m_bufferX->Size();
        }

        // DISALLOW_COPY_AND_ASSIGN
        AutoBuffer(const AutoBuffer&);
        void operator=(const AutoBuffer&);
    };
    

    const UINT64 UNKNOWN_BLOCKDEVICE_LEN = (UINT64)-1;

    // A set of buffer pin modes
    enum BufferPinModes
    {
        BPM_Read,
        BPM_Insert,
        BPM_ReadInsert,
    };

    // A set of buffer access states
    enum BufferAccessStates
    {
        BAS_Ok,
        BAS_Invalid, // exception raised
        BAS_EntryNotExist, // not found in cache
        BAS_EntryAlreadyExist, // already in cache, readable
        BAS_EntryAlreadyUnderConstruction //already in cache, but still under construction
    };

    // Forward decl
    class BlockDevice;
    class StreamingOutputChannel;

    class SCOPE_ENGINE_API BufferGroup
    {
    public:
        virtual void Close() = 0;
        virtual UINT64 GetLength() = 0;
        virtual void CacheLength(__in const UINT64 len) = 0;
        virtual const PooledBufferDescriptor* Pin(__in const UINT64 offset, __in const SIZE_T size, __in const BufferPinModes pinMode, __out BufferAccessStates& accessState) = 0;
        virtual void EndPin(__in const PooledBufferDescriptor* desc) = 0;

        virtual bool IsPinned(__in const BufferDescriptor* desc) = 0;
        // In case a cache entry was pinned for write, the user may fail to fill this entry; in this case, Invalidate() is called to indicate to discard this entry.
        virtual void Invalidate(__in const BufferDescriptor* desc) = 0;
        virtual void SetBufferStat(__in const BufferDescriptor* desc, __in const SIZE_T contentSize, __in const uint32_t userCacheFlags) = 0;
        virtual void RePin(__in const BufferDescriptor* desc, __in const CachePinModes pinMode) = 0;

        virtual ~BufferGroup() {}
    };

    class SCOPE_ENGINE_API BufferPool
    {
    public:
        virtual bool Init(
            __in const CacheOptions& initOptions,
            __in const uint64_t poolSize = 0,
            __in ICachePool* cacheProvider = nullptr) = 0;
        virtual BufferGroup* Open(__in BlockDevice* srcBlockDevice) = 0;
        virtual void Shutdown() = 0;
        virtual ICachePool* GetCacheProvider() = 0;
        virtual void DumpExecutionStats() = 0;
        static BufferPool* GetGlobal();
        static BufferPool* GetDummy();

        virtual ~BufferPool() {}
    };

    //////////////////////
    // I/O Manager section
    //////////////////////

    // Definition of Security keys
    class SecurityKeys
    {
    public:
        enum SecurityKeyType
        {
            NoKeys,        // No keys have been provided
            HaveKeys,      // Valid keys are available
            DebugKeys,     // Debugging a vertex and we don't have keys
            CacheKeys,     // Don't have keys for the cache, this will need to be fixed
            ManagedKeys,   // Managed entry point, no keys supplied (could be fixed)
            JMNoKeys,      // JM passed NULL keys
            TestKeys,      // Keys provided by a test
            BajaKeys       // Baja keys
        };

        // Create a real security key from the buffer and length passed
        // - the constructor takes a copy of the keys
        // - this is expected to be the only copy, all others are references to it via the shared_ptr
        SecurityKeys(void *buffer, SIZE_T length)
        {
            // JM doesn't always pass valid keys, work around that here
            if (buffer == nullptr)
            {
                m_type = JMNoKeys;
                m_keys.reset();
            }
            else
            {
                m_type = HaveKeys;
                m_keys = std::make_shared<std::vector<BYTE>>((BYTE *)buffer, (BYTE *)buffer + length);
            }
        }

        // Create a fake key for code paths that don't receive a real one
        // - the type parameter indicates which code path created it
        SecurityKeys(SecurityKeyType type) :
            m_type(type), m_keys(nullptr)
        {
        }

        SIZE_T GetLength()
        {
            SCOPE_ASSERT(m_type != NoKeys);
            return (m_type == HaveKeys) ? m_keys->size() : 0;
        }

        void * GetBuffer()
        {
            SCOPE_ASSERT(m_type != NoKeys);
            return (m_type == HaveKeys) ? m_keys->data() : nullptr;
        }

    private:
        SecurityKeyType                        m_type;
        std::shared_ptr<std::vector<BYTE>>     m_keys;
    };

    // The struct collects all available statistics about the block device
    struct BlockDeviceStatistics
    {
        LONGLONG  m_ioOpenTime;
        LONGLONG  m_ioCloseTime;
        LONGLONG  m_ioReadWriteTime;
        INT64     m_processedBytes;
        LONG      m_totalOperations;
        LONGLONG  m_ioThrottlingTime;
        LONGLONG  m_ioRetryTime;
        LONGLONG  m_ioTotalThrottlingTime;
        LONGLONG  m_ioTotalRetryTime;
        double    m_congestionProbability;
        
        BlockDeviceStatistics() { Reset(); }
        
        void Reset() { m_ioOpenTime = m_ioCloseTime = m_ioReadWriteTime = m_ioThrottlingTime = m_ioRetryTime = m_ioTotalThrottlingTime = m_ioTotalRetryTime = m_processedBytes = m_totalOperations  = 0; 0; m_congestionProbability = 0; }
    };
    
    class SCOPE_ENGINE_API IOManager
    {
    public:
        static const SIZE_T x_defaultInputBufSize = 4194304; // 4Mb
        static const SIZE_T x_minInputBufSize = 4096; // 4Kb
        static const int    x_defaultInputBufCount = 2;
        static const SIZE_T x_defaultOutputBufSize = 4194304; // 4Mb
        static const SIZE_T x_minOutputBufSize = 4096; // 4Kb
        static const int    x_defaultOutputBufCount = 16;
        static const int    x_readThrottlingLimit = 250; // UnionAll throttling

        virtual void AddInputStream(const std::string& node, const std::string& path) = 0;
        virtual void AddOutputStream(const std::string& node, const std::string& path) = 0;
        virtual void AddOutputStream(const std::string& node, const std::string& path, const std::string& affinity) = 0;
        virtual void AddOutputStream(const std::string& node, const std::string& path, const std::string& affinity, const INT64 expirationTime) = 0;
        virtual void AddStreamingStream(const std::string& node, const std::string& path, const INT64 expirationTime, bool sealStream) = 0;
        virtual void AddInputGetSStream(UINT64 offset, UINT64 length, const std::string& node, const std::string& path) = 0;
        virtual BlockDevice* GetDevice(const std::string& node) = 0;
        virtual std::vector<BlockDevice*> GetSStreamDevices(int oid) = 0;
        virtual std::vector<int> GetSStreamPartitionIds(int oid) = 0;
        virtual std::vector<int> GetSStreamProcessingGroupIds(int oid) = 0;
        virtual std::vector<BlockDevice*> GetDevices(const std::string& path) = 0;
        virtual StreamingOutputChannel* GetStreamingOutputChannel(const std::string& node) = 0;
        virtual void RemoveSStream(int oid) = 0;
        virtual void RemoveStream(const std::string& node, const std::string& path) = 0;
        virtual void Init(const SecurityKeys keys) = 0;
        virtual void Reset() = 0;
        
        virtual void SetIOPriority(int ioPriority) = 0; 
        virtual void SetPreferSSD(bool preferSSDForTempData, bool preferSSDForGlobalData) = 0; 
        virtual bool GetPreferSSD(bool forTempData) = 0;

        virtual bool GetStreamState(const std::string & path, bool & isOpened, CsError & errorCode, string & errorText, BlockDeviceStatistics * deviceStats) = 0;

        static std::string GetTempStreamName();
        static bool IsLocalVolumeStreamName(const std::string& path);
        static void CopyStream(BlockDevice * inputDevice, BlockDevice * outputDevice);

        static IOManager* GetGlobal();
        static void EnableIScopeNonBlockingStreaming();

        virtual ~IOManager() {}
    };

    class SCOPE_ENGINE_API Scanner
    {
    public:
        struct SCOPE_ENGINE_API Statistics
        {
            LONGLONG    m_ioOpenTime;
            LONGLONG    m_ioCloseTime;
            LONGLONG    m_ioTime;

            LONGLONG    m_ioMinTime;
            LONGLONG    m_ioAvgTime;
            LONGLONG    m_ioMaxTime;

            LONGLONG    m_ioWaitTime;
            LONGLONG    m_operations;

            SIZE_T      m_ioMinTimeSize; // size of the request that took the minimum time
            SIZE_T      m_ioMaxTimeSize; // size of the request that took the maximum time
            SIZE_T      m_ioTotalSize;

            SIZE_T      m_memorySize; // combined size of all buffers

            void ConvertToMilliSeconds();
            LONGLONG GetInclusiveTimeInMilliSeconds();
            void Reset();

            // Constructor clears all data members.
            Statistics() { Reset(); }
        };

        enum ScannerType
        {
            STYPE_ReadOnly = 1,
            STYPE_OpenExistAndAppend,
            STYPE_Create
        };

        static Scanner* CreateScanner(BlockDevice* device, MemoryManager* memMgr, ScannerType type, __in SIZE_T reserveSize, SIZE_T bufferSize, UINT numOfBuffers);
        static Scanner* CreateScanner(BlockDevice* device, MemoryManager* memMgr, ScannerType type, __in SIZE_T reserveSize, SIZE_T bufferSize, UINT numOfBuffers, bool preferSDD);
        static void DeleteScanner(Scanner *scanner);

        virtual void Open(bool startReadAhead = false, bool cancelable = false) = 0;
        virtual void Close() = 0;
        virtual std::string GetStreamName() const = 0;

        virtual void Start(__in UINT64 initialOffset = 0) = 0;
        virtual void Finish() = 0;
        virtual bool GetNext(__out const BufferDescriptor*& desc, __out SIZE_T& size) = 0;
        virtual void PutNext(__in const BufferDescriptor* desc, __in SIZE_T size, __in bool seal = false) = 0;
        virtual void Expand(__in const BufferDescriptor* desc, __in SIZE_T size) = 0;
        virtual UINT64 Length() = 0;

        virtual HANDLE GetWaitValid() const = 0;
        virtual void WaitForAllPendingOperations() = 0;
        virtual GUID InitializeStreamAffinity() = 0;

        virtual Statistics GetStatistics() = 0;
        virtual void WriteRuntimeStats(TreeNode & root) = 0;

        virtual bool IsOpened() const = 0;
        virtual void SaveState(string& stateString, UINT64 position) = 0;
        virtual void LoadState(const string& stateString) = 0;

        virtual ~Scanner() {}
    };

    class MemoryInputStream;
    class MemoryOutputStream;

    class SCOPE_ENGINE_API BloomFilter
    {
    public:
        static const UINT MagicNumValue = 0xABDDF53D;
        static const UINT DefaultSeed = 0xD94C5FE3;
        static const UINT DefaultBitCount = (1024 * 1024) * 8;     // 1 MB
        static const UINT MaxBitCount = ((UINT)4 * 1024 * 1024) * 8;  // 4 MB
        static const UINT DefaultHashCount = 4;
        static const UINT64 HashTable[];
        static const SIZE_T HashTableSize = 4096;

        enum Version
        {
            VersionV1 = 1,
        };

        enum State
        {
            StateInvalid = 0x00,
            StateRead    = 0x01,
            StateWrite   = 0x02,
        };

#pragma pack(push, 1)
        struct SCOPE_ENGINE_API Header
        {
            UINT MagicNumber;
            UINT Version;
            UINT BitCount;
            UINT HashCount;
            UINT KeyCount;
            UINT Checksum;

            Header(
                __in_opt UINT bitCount = DefaultBitCount,
                __in_opt UINT hashCount = DefaultHashCount);

            __checkReturn bool IsValid() const;
        };
#pragma pack(pop)

        struct SCOPE_ENGINE_API ColumnData
        {
            const BYTE* Data;
            SIZE_T Length;
        };

    public:
        BloomFilter();

        BloomFilter(
            __in UINT bitCount,
            __in_opt UINT hashCount = DefaultHashCount);

        BloomFilter(const BloomFilter& other);

        ~BloomFilter();

        BloomFilter& operator=(const BloomFilter& other);

        void Initialize(
            __in_opt UINT bitCount = DefaultBitCount,
            __in_opt UINT hashCount = DefaultHashCount);

        bool Deserialize(
            __in_bcount(length) const BYTE* buffer,
            __in SIZE_T length);

        void Serialize(
            __out std::unique_ptr<BYTE[]>& buffer,
            __out SIZE_T& length);

        void Insert(__in const std::vector<struct BloomFilter::ColumnData>& keys);
        bool Lookup(__in const std::vector<struct BloomFilter::ColumnData>& keys) const;

        const Header& GetHeader() const {return m_header;}
        bool IsValid() const {return (m_state != StateInvalid) && m_header.IsValid();}
        bool IsEmpty() const {return m_header.KeyCount == 0;}
        std::string ToString() const;

        // Gets the false positive error rate of this instance
        //
        double GetError() const {return CalculateError(m_header.BitCount, m_header.KeyCount, m_header.HashCount);}

        // Suggests bit counts needed to meet false positive error tolerance.
        // It is not guaranteed that returned bit count is multiple of 8.
        static UINT SuggestBitCount(
            __in double errorTolerance,
            __in UINT keyCount,
            __in_opt UINT hashCount = DefaultHashCount);

        // Calculates false positive error rate as percentage
        //
        static double CalculateError(
            __in UINT bitCount,
            __in UINT keyCount,
            __in UINT hashCount);

    protected:
        __inline bool GetBit(__in UINT bitIndex) const;

        __inline void SetBit(__in UINT bitIndex);

        __inline UINT GetBitVectorSize() const;

        __inline void CreateBitVector();

        UINT64 CalculateHash(
            __in UINT64 initialValue,
            __in_bcount(length) const BYTE* key,
            __in SIZE_T length) const;

        UINT64 CalculateHash(
            __in UINT64 initialValue,
            __in const std::vector<BloomFilter::ColumnData>& keys) const;

        UINT CalculateChecksum(
            __in_bcount(length) const BYTE* key,
            __in SIZE_T length) const;

    protected:
        BYTE m_state;
        Header m_header;
        BYTE* m_bitVector;
    };

    //////////////////////
    // Structured streams format V3
    //////////////////////
    namespace SSLibV3
    {
        // helpers
        inline UINT VarDecodeInt32(BYTE* buffer, UINT& value)
        {
            UINT pos = 0;
            UINT digit = 0;
            BYTE b;
            value = 0;

            do
            {
                b = buffer[pos++];
                value |= (b & 0x7f) << digit;
                digit += 7;
            } while ((b & 0x80) != 0);

            return pos;
        }

        // The iterator class pointing to a single column
        //
        // It has 3 parts
        //   1) a data pointer - it is always set
        //   2) a length pointer - points to encoded length values, it is used only by variable size columns, otherwise it is null
        //   3) a nulls pointer - points to a null bitmap, if there are not any NULL values then the bitmap is optimized away and the pointer is null
        //
        class ColumnIterator
        {
            BYTE* m_dataPtr;
            BYTE* m_lenPtr;
            BYTE* m_nullPtr;
            BYTE  m_nullPos;
            // Cached values
            UINT  m_lenLen;
            UINT  m_currentLength;
            bool  m_currentIsNull;

            void UpdateCachedState()
            {
                m_currentIsNull = m_nullPtr ? (*m_nullPtr & (1 <<  m_nullPos)) != 0 : false;
                m_currentLength = 0;
                m_lenLen = (m_lenPtr == nullptr) ? 0 : VarDecodeInt32(m_lenPtr, m_currentLength); 
            }
        public:
            ColumnIterator() {}
            ColumnIterator(BYTE* dataPtr, BYTE* lenPtr, BYTE* nullPtr, BYTE nullPos) : m_dataPtr(dataPtr), m_lenPtr(lenPtr), m_nullPtr(nullPtr), m_nullPos(nullPos)
            {
                UpdateCachedState();
            }

            ColumnIterator(BYTE* block, UINT nullOffset, BYTE nullPos, UINT dataOffet, UINT lenOffset)
            {
                if (nullOffset != -1)
                {
                    m_nullPtr = block + nullOffset;
                    m_nullPos = nullPos;
                }
                else
                {
                    m_nullPtr = nullptr;
                    m_nullPos = 0;
                }

                m_dataPtr = block + dataOffet;

                if (lenOffset != -1)
                {
                    m_lenPtr = block + lenOffset;
                }
                else
                {
                    m_lenPtr = nullptr;
                }

                UpdateCachedState();
            }

            BYTE* DataRaw() const
            {
                return m_dataPtr;
            }

            template<typename T>
            T Data() const
            {
                return *reinterpret_cast<T*>(m_dataPtr);
            }

            bool IsNull() const
            {
                return m_currentIsNull;
            }

            UINT Length() const
            {
                return m_currentLength;
            }

            // Set of Advance methods with a varying degree of specialization
            //
            // Advances a null values pointer, if present
            // Only true not nullable types can skip calling this method
            void IncrementNull()
            {
                if (m_nullPtr)
                {
                    ++m_nullPos;
                    if (m_nullPos == 8)
                    {
                        m_nullPos = 0;
                        ++m_nullPtr;
                    }
                }
                UpdateCachedState();
            }

            // Advance a data pointers for not nullable fixed size types when we know 
            // that a null values pointer will not be present.
            // In theory, passing a negative number will move pointer backwards 
            template <UINT length>
            void IncrementFixedNotNullable()
            {
                m_dataPtr += length;
                // The cached state is still OK
            }

            // Advance a data pointer for nullable fixed size types
            // Note: a value must be present (i.e. must not be NULL)
            template <UINT length>
            void IncrementFixed()
            {
#if defined(SCOPE_DEBUG)
                // It would be great to have this turned on by default but it's expensive
                SCOPE_ASSERT(!IsNull());
#endif
                IncrementFixedNotNullable<length>();
                IncrementNull();
            }

            // Advance a data pointer for variable size types (that are by definition nullable)
            // Note: a value must be present (i.e. must not be NULL)
            void IncrementVariable()
            {
#if defined(SCOPE_DEBUG)
                // It would be great to have this turned on by default but it's expensive
                SCOPE_ASSERT(!IsNull());
#endif
                m_lenPtr += m_lenLen;
                m_dataPtr += m_currentLength;
                IncrementNull();
            }

            // A general purpose advance method used by the library, not by the codegen
            void Increment(UINT length)
            {
                if (m_nullPtr)
                {
                    bool null = IsNull();
                    ++m_nullPos;
                    if (m_nullPos == 8)
                    {
                        m_nullPos = 0;
                        ++m_nullPtr;
                    }
                    if (null)
                    {
                        UpdateCachedState();
                        return;
                    }
                }

                if (length == 0)
                {
                    length = m_currentLength;
                    m_lenPtr += m_lenLen;
                }

                m_dataPtr += length;
                UpdateCachedState();
            }
        };

        // Forward declaration
        class Block;

        struct DataUnitDescriptor
        {
            BYTE* m_dataColumnSizes;
            BYTE* m_indexColumnSizes;
            UINT  m_dataColumnCnt;
            UINT  m_indexColumnCnt;

            UINT* m_sortKeys;
            UINT  m_sortKeysCnt;

            UINT  m_numOfBuffers;
            bool  m_descending;
            bool  m_skipUnavailable;

            UINT  m_blockSize;

            BYTE  m_numBloomFilterKeys;

            DataUnitDescriptor() :
                m_dataColumnSizes(NULL),
                m_indexColumnSizes(NULL),
                m_dataColumnCnt(0),
                m_indexColumnCnt(0),
                m_sortKeys(NULL),
                m_sortKeysCnt(0),
                m_numOfBuffers(0),
                m_descending(false),
                m_skipUnavailable(false),
                m_blockSize(0),
                m_numBloomFilterKeys(0)
            {}

            DataUnitDescriptor(BYTE* dataColumnSizes, BYTE* indexColumnSizes, UINT* sortKeys, UINT dataColumnCnt, UINT indexColumnCnt, UINT sortKeysCnt, UINT numOfBuffers, bool descending, bool skipUnavailable, UINT blockSize, BYTE numBloomFilterKeys = 0) :
                m_dataColumnSizes(dataColumnSizes),
                m_indexColumnSizes(indexColumnSizes),
                m_dataColumnCnt(dataColumnCnt),
                m_indexColumnCnt(indexColumnCnt),
                m_sortKeys(sortKeys),
                m_sortKeysCnt(sortKeysCnt),
                m_numOfBuffers(numOfBuffers),
                m_descending(descending),
                m_skipUnavailable(skipUnavailable),
                m_blockSize(blockSize),
                m_numBloomFilterKeys(numBloomFilterKeys)
            {
            }
        };

        class SCOPE_ENGINE_API Provenance
        {
        public:
            static Provenance* CreateProvenance(const string& baseStreamGuid);
            virtual void Serialize(std::ostream * writer) const = 0;
            virtual void Deserialize(std::istream * reader) = 0;
            virtual ~Provenance() {}
        };

        struct ColumnGroupInfo 
        {
            std::string m_columnGroupSchema;
            int m_columnGroupIndex;
        };

#pragma pack(push,1)
        // A row in Dataunit Description Table.
        struct DataunitTableRow
        {
            int 	m_PartitionIndex;
            int 	m_ColumnGroupIndex;
            INT64	m_Offset;
            INT64	m_Length;
            INT64	m_DataLength;
            INT64	m_RowCount;
        };
#pragma pack(pop)

        // A row in Partition Info Table.
        struct PartitionInfoTableRow
        {
            std::string     m_PartitionKeyRange;	// byte[]
            int    		    m_BeginPartitionIndex;
            std::string     m_AffinityId;
        };

        class SCOPE_ENGINE_API DataUnitScanner
        {
        public:
            struct CacheStatistics
            {
                LONGLONG m_dataBlocksFromDisk;
                LONGLONG m_dataBlocksFromCache;
                LONGLONG m_indexBlocksFromDisk;
                LONGLONG m_indexBlocksFromCache;
            };

            struct MemoryStatistics
            {
                SIZE_T      m_maxAllocatedMemoryIndex; // sum for all index buffers, theoretical max if all buffers hold their max allocation simultaneously
                SIZE_T      m_allocationCountIndex; // sum for all index buffers
                SIZE_T      m_maxAllocatedMemoryData; // sum for all data buffers, theoretical max if all buffers hold their max allocation simultaneously
                SIZE_T      m_allocationCountData; // sum for all data buffers
            };

        public:
            static DataUnitScanner* CreateScanner(BlockDevice* device, MemoryManager* memMgr, BufferPool* bp);
            static DataUnitScanner* CreateScanner(BlockDevice* device, UINT64 offset, MemoryManager* memMgr, BufferPool* bp);
            static DataUnitScanner* CreateScanner(BlockDevice* device, UINT64 offset, UINT64 length, MemoryManager* memMgr, BufferPool* bp);
            static void DeleteScanner(DataUnitScanner* scanner);

            virtual void Open(const DataUnitDescriptor&) = 0;
            virtual void Close() = 0;

            virtual Block* GetNextBlock(Block* prev) = 0;
            virtual ColumnIterator GetIterator(Block* b, UINT columnId) = 0;
            virtual UINT GetRowCount(Block* b) const = 0;
            virtual UINT64 GetSkippedDataLength() const = 0;
            virtual void MarkRemainderUnavailable() = 0;

            virtual void SetLowBound(const std::function<bool(ColumnIterator*)>& predicate) = 0;
            virtual void SetHiBound(const std::function<bool(ColumnIterator*)>& predicate) = 0;

            virtual void ResetStatistics() = 0;
            virtual void Reset() = 0;

            virtual Scanner::Statistics GetStatistics() = 0;
            virtual CacheStatistics GetCacheStatistics() = 0;
            virtual MemoryStatistics GetMemoryStatistics() = 0;
            virtual void WriteRuntimeStats(TreeNode & root) = 0;
            virtual BloomFilter* GetBloomFilter() = 0;

            virtual ~DataUnitScanner() {}
        };

        // default structure stream data/index block size 4MB
        const unsigned __int32 DEFAULT_SS_BLOCK_SIZE = 4 * 1024 * 1024;

        class PartitionMetadata;
        class KeySampleCollection;
        class SCOPE_ENGINE_API SStreamStatistics
        {
        public:
            static SStreamStatistics * Create(const char* columnNames[], int columnCount);
            virtual void CollectDataBlock() = 0;
            virtual void CollectRow(UINT64 rowSize) = 0;
            virtual void CollectColumn(int columnId, UINT64 columnSize) = 0;
            virtual std::string DumpToXmlString( ) const = 0;
            virtual void BuildFromXmlString(std::string& xmlStr) = 0;
            virtual void Merge(SStreamStatistics* statistics) = 0;
            virtual void Merge(SStreamStatistics* statistics, bool isInPartition) = 0;
            virtual void SetKeySampleCollection(std::shared_ptr<KeySampleCollection>& collection) = 0;
            virtual ~SStreamStatistics() {}
        };

        // Callback function to serialize a row to SS block
        // Requirements:
        //  HANDLE rowHandle : 
        //      typeless handle of the row, 
        //      the callback is responsible for casting the handle to proper type.
        //  AutoBuffer* buffer: 
        //      the page buffer
        //      format of the row: nullBitmap, column0, column 1, ...
        //  int* offsets: 
        //      an array of columnCount + 1 integers 
        //      upon returning from the callback,
        //      the array should hold the offset of each column relative to row start
        //      in the first [columnCount] elements. 
        //      The last element should hold the total length of the row.
        //  arraySize: 
        //      The number of element in offsets array. for safety only.
        //      The callback function should assert that the arraySize is equal to columnCount + 1
        typedef void (*SerializeRowCallback)(HANDLE, AutoBuffer*, int*, SIZE_T arraySize);
        
        class SCOPE_ENGINE_API DataUnitWriter
        {
        public:
            static DataUnitWriter* CreateWriter(Scanner* scanner, MemoryManager* memMgr, UINT BloomFilterBitCount);

            virtual void Open( const DataUnitDescriptor& desc, 
                                SerializeRowCallback serializeRowCallback,
                                const char* dataSchema,
                                const char* indexSchema,
                                const GUID& affinityID,
                                SStreamStatistics* stat) = 0;
            virtual void Close(bool needToSeal = true) = 0;
            virtual void AppendRow(HANDLE rowHandle) = 0;
            virtual void Flush() = 0;

            // Total Length 
            virtual UINT64 Length() const = 0;  

            // Total data length, (data pages and index pages, exclude metadata)
            virtual UINT64 DataLength() const = 0;

            // Number of rows.
            virtual UINT64 RowCount() const = 0;

            virtual void WriteRuntimeStats(TreeNode & root) = 0;

            virtual ~DataUnitWriter() {}
        };

        SCOPE_ENGINE_API std::string GenerateDataSchema(
            const char  *   columnNames[],
            const char  *   columnTypes[],
            SIZE_T          columnCount,
            const char  *   sortIsUnique,
            UINT *          sortColumnIds,
            const char *    sortOrders[],
            SIZE_T          sortColumnCount,
            const char *    partType,
            UINT *          partColumnIds,
            const char *    partOrders[],
            SIZE_T          partColumnCount);
            
        SCOPE_ENGINE_API std::string GenerateIndexSchema(
            const char  *   columnNames[],
            const char  *   columnTypes[],
            SIZE_T          columnCount,
            UINT *          sortColumnIds,
            const char *    sortOrders[],
            SIZE_T          sortColumnCount);
    }

    ////////////////////////////////////
    // Stream payload metadata
    ////////////////////////////////////
    class BinaryOutputStream;
    class SCOPE_ENGINE_API PartitionMetadata
    {
    public:
        static const __int64 PARTITION_NOT_EXIST = -2;

        virtual ~PartitionMetadata() {}

        virtual __int64 GetPartitionId() const = 0;

        virtual int GetMetadataId() const = 0;

        virtual void Serialize(BinaryOutputStream * stream) = 0;

        virtual void WriteRuntimeStats(TreeNode & root) = 0;
        
        static bool Equals(PartitionMetadata * x, PartitionMetadata * y)
        {
            return x->GetMetadataId() == y->GetMetadataId() &&
                   x->GetPartitionId() == y->GetPartitionId();
        }

        template <typename OperatorArr>
        static PartitionMetadata * MergeMetadata(OperatorArr & children, size_t count)
        {
            if (count == 0)
            {
                return NULL;
            }
            
            PartitionMetadata * metadata = children[0]->GetMetadata();

            if (count > 1)
            {
                std::vector<PartitionMetadata*> partMetadata;
                for(size_t i = 1; i < count; ++i)
                {
                    partMetadata.push_back(children[i]->GetMetadata());
                }

                if (metadata == NULL)
                {
                     for (int i = 0; i < count - 1; i++)
                     {
                          if (partMetadata[i] != NULL)
                          {
                              throw MetadataException("Metadata is not consistent");
                          }
                     }
                }
                else
                {
                    metadata = metadata->Merge(partMetadata);
                    if (!metadata)
                    {
                        throw MetadataException("Metadata is not consistent");
                    }
                }
            }

            return metadata;
        }

        template <typename OperatorPtr>
        static PartitionMetadata * UnionMetadata(std::vector<OperatorPtr> & children)
        {
            PartitionMetadata * metadata = children[0]->GetMetadata();

            // UNDONE - check consistency of the metadata
            // Unfortunately, when UnionMetadata is called the children are not opened
            // as we must not open all children at once due to the efficiency reasons
            // i.e. we run of memory if we try to Init thousands of children (not uncommon)

            return metadata;
        }

        template <typename OperatorPtr>
        static PartitionMetadata * UnionMetadata(OperatorPtr * children)
        {
            PartitionMetadata * metadata = children[0].GetMetadata();

            // UNDONE - check consistency of the metadata
            // Unfortunately, when UnionMetadata is called the children are not opened
            // as we must not open all children at once due to the efficiency reasons
            // i.e. we run of memory if we try to Init thousands of children (not uncommon)

            return metadata;
        }

        virtual PartitionMetadata* Merge(const std::vector<PartitionMetadata*>& partMetadata) const
        {
            for (int i = 0; i < partMetadata.size(); i++)
            {
                if (!partMetadata[i])
                {
                    continue;
                }
                else if (GetMetadataId() != partMetadata[i]->GetMetadataId() && GetPartitionId() != partMetadata[i]->GetPartitionId())
                {
                    return NULL;
                }
            }
            
            return const_cast<PartitionMetadata*>(this);
        }
    };

    class SCOPE_ENGINE_API SStreamMetadata : public PartitionMetadata
    {
    public:
        static SStreamMetadata * Create(MemoryManager * memMgr);
            
        virtual void Initialize(const std::string & streamSchema,
            std::shared_ptr<std::vector<SSLibV3::ColumnGroupInfo>>& columnGroupInfo,
            std::shared_ptr<std::vector<SSLibV3::DataunitTableRow>>& dataunitInfo,
            std::shared_ptr<std::vector<SSLibV3::PartitionInfoTableRow>>& partitionInfo,
            std::shared_ptr<SSLibV3::SStreamStatistics>& stat) = 0;                
        virtual void SetProvenance(std::shared_ptr<SSLibV3::Provenance>& provenance) = 0;
        virtual void SetUserData(const std::string& userData) = 0;
        virtual void UpdateDataUnitOffset() = 0;
        virtual void PreferSSD(bool preferSSD) = 0;
        virtual void Write(BlockDevice* device) = 0;
        virtual void Read(BlockDevice* device) = 0;
        virtual void UpdatePartitionIndex(int partitionIndex) = 0;
        virtual const ULONG GetSStreamFormatVersion() const = 0;
        virtual std::shared_ptr<const std::vector<SSLibV3::DataunitTableRow>> GetDataUnitInfo() const = 0;
        virtual std::shared_ptr<const std::vector<SSLibV3::PartitionInfoTableRow>> GetPartitionInfo() const = 0;
        virtual std::shared_ptr<const std::vector<SSLibV3::ColumnGroupInfo>> GetColumnGroupInfo() const = 0;
        virtual const std::string& GetSchemaString() const = 0;
        virtual const std::string& GetUserData() const = 0;
        virtual std::shared_ptr<const SSLibV3::SStreamStatistics> GetSStreamStatistics() const = 0;
        virtual std::shared_ptr<const SSLibV3::Provenance> GetProvenance() const = 0;

        virtual ~SStreamMetadata() {}

        // interfaces from PartitionMetadata
        __int64 GetPartitionId() const { throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "GetPartitionId not work for StreamMetadata!"); }

        int GetMetadataId() const { throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "GetMetadataId not work for StreamMetadata!"); }

        void Serialize(BinaryOutputStream * stream)
        {
            (stream);
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Serialize not work for StreamMetadata!");
        }

        virtual void WriteRuntimeStats(TreeNode &)
        {
        }
    };

    ////////////////////////////////////
    // User Defined Type manager
    ////////////////////////////////////
    class ScopeManagedHandle;
    class ScopeUDTColumnType;
    class TextOutputStreamBase;

    class SCOPE_ENGINE_API UDTManager
    {
    public:
        virtual void CreateScopeUDTObject(ScopeManagedHandle & managedHandle, int udtId) = 0;
        virtual void CopyScopeUDTObject(const ScopeUDTColumnType & from, ScopeUDTColumnType & to) = 0;
        virtual bool CheckNullScopeUDTObject(const ScopeUDTColumnType & handle) = 0;
        virtual void SetNullScopeUDTObject(ScopeUDTColumnType & handle) = 0;
        virtual void FStringToScopeUDTColumnType(const char * str, ScopeUDTColumnType & handle) = 0;
        virtual void SerializeUDT(const ScopeUDTColumnType & handle, TextOutputStreamBase * out) = 0;

        static UDTManager * GetGlobal();
        static void Reset(UDTManager * manager = nullptr);

        virtual ~UDTManager() {}
    };

    //////////////////////
    // Thread pool
    //////////////////////
    struct ThreadPoolData;
    class SCOPE_ENGINE_API PrivateThreadPool
    {
        ThreadPoolData   *   m_threadPoolData;
        bool                 m_initialized;

        void Init();

    public:
        PrivateThreadPool(bool nativeOnly);
        ~PrivateThreadPool();

        BOOL SetThreadpoolMax(DWORD max);
        BOOL SetThreadpoolMin(DWORD min);

        bool QueueUserWorkItem(void (*cb)(PVOID), PVOID pv);
        bool QueueUserWorkItem(void (*cb)(PVOID), PVOID pv, HANDLE returnEventHandle);

        void WaitForAllCallbacks(bool cancelNotStarted);
        void FlushAllCallbacks();
    };

    
    // Utility functions that do not need to understand types defined in ScopeContainer.h
    SCOPE_ENGINE_API std::string GuidToString(const GUID & guid);
    SCOPE_ENGINE_API bool GuidFromString(const char * str, unsigned int size, GUID & guid); // to avoid use GUID_NULL litteral in ScopeOperator.h

    //////////////////////////////
    // Runtime statistics
    //////////////////////////////

    class SCOPE_ENGINE_API TreeNode
    {
    public:
        static std::unique_ptr<TreeNode> CreateElement(const std::string & name);

        virtual TreeNode & AddElement(const std::string & name) = 0; 
        virtual void AddElement(TreeNode&& node) = 0;
        virtual void AddAttribute(const std::string & name, LONGLONG value) = 0;
        virtual bool TryGetAttributeValue(const std::string & name, LONGLONG & value) = 0;
        virtual std::string ToXml() const = 0;
        virtual std::string ToString() const = 0;

        virtual ~TreeNode() {}
    };

    struct SCOPE_ENGINE_API RuntimeStats
    {
        static const char * const InclusiveTime();  // "inclusiveTime";
        static const char * const ExclusiveTime();  // "exclusiveTime";
        static const char * const IOTime();         // "ioTime";
        static const char * const RowCount();       // "rowCount";
        static const char * const MaxRowCount();	// "maxRowCount";
        static const char * const KeyCount();       // "keyCount";
        static const char * const MaxKeyCount();	// "maxKeyCount";
        static const char * const OperatorId();     // "opId";
        static const char * const MaxInputCount();     // "maxInputCount";
        static const char * const AvgInputCount();     // "avgInputCount";
        static const char * const MaxOutputCount();     // "maxOutputCount";
        static const char * const AvgOutputCount();     // "avgOutputCount";

// Join statistics
        static const char * const MaxAvgJoinProduct(); // "maxAvgJoinProduct"

// IncrementalAllocator/InteropAllocator/ExpandableBuffer statistics
        static const char * const MaxReservedSize();   // "maxReservedSize"
        static const char * const MaxCommittedSize();  // "maxCommittedSize"
        static const char * const MaxCommitCount();    // "maxCommitCount"
        static const char * const MaxResetCount();     // "maxResetCount"
        static const char * const MaxStringSize();     // "maxStringSize"
        static const char * const MaxBinarySize();     // "maxBinarySize"
        static const char * const MaxRowDataSize();    // "maxRowDataSize"
        static const char * const MaxFixedRowDataSize();  // "maxFixedRowDataSize"
        static const char * const AvgReservedSize();   // "avgReservedSize"
        static const char * const AvgCommittedSize();  // "avgCommittedSize"
        static const char * const AvgStringSize();     // "avgStringSize"
        static const char * const AvgBinarySize();     // "avgBinarySize"
        static const char * const AvgRowDataSize();    // "avgRowDataSize"

// input/output buffers statistics
        static const char * const MaxBufferCount();    // "maxBufferCount"
        static const char * const MaxBufferSize();     // "maxBufferSize"
        static const char * const MaxBufferMemory();   // "maxBufferMemory"
        static const char * const AvgBufferMemory();   // "avgBufferMemory"

// SStreamOutputStream statistics
        static const char * const MaxPartitionKeyRangeSize();    // "maxPartitionKeyRangeSize"

// Sort/ParallelLoad operator and PageConvertor statistics
        static const char * const MaxPeakInMemorySize();    // "maxPeakInMemorySize"
        static const char * const AvgPeakInMemorySize();    // "avgPeakInMemorySize"

// Sorter statistics
        static const char * const MaxPeakInMemorySizeRead();      // "maxPeakInMemorySizeRead"
        static const char * const AvgPeakInMemorySizeRead();      // "avgPeakInMemorySizeRead"
        static const char * const MaxPeakInMemorySizePreFetch();  // "maxPeakInMemorySizePreFetch"
        static const char * const AvgPeakInMemorySizePreFetch();  // "avgPeakInMemorySizePreFetch"
        static const char * const MaxPeakInMemorySizeFetch();     // "maxPeakInMemorySizeFetch"
        static const char * const AvgPeakInMemorySizeFetch();     // "avgPeakInMemorySizeFetch"
        static const char * const MaxFillNewBucketCount();        // "maxFillNewBucketCount"
        static const char * const AvgFillNewBucketCount();        // "avgFillNewBucketCount"
        static const char * const MaxMergeBucketCount();          // "maxMergeBucketCount"
        static const char * const AvgMergeBucketCount();          // "avgMergeBucketCount"
        static const char * const MaxSpillBucketCount();          // "maxSpillBucketCount"
        static const char * const AvgSpillBucketCount();          // "avgSpillBucketCount"
        static const char * const MaxFinalBucketCount();          // "maxFinalBucketCount"
        static const char * const AvgFinalBucketCount();          // "avgFinalBucketCount"
        static const char * const MaxNewInMemoryBucketCount();    // "maxNewInMemoryBucketCount"        

// MemoryManager statistics
        static const char * const MaxOverallMemoryPeakSize();    // "maxOverallMemoryPeakSize"
        static const char * const MaxGlobalMemoryPeakSize();     // "maxGlobalMemoryPeakSize"
        static const char * const MaxIOMemoryPeakSize();         // "maxIOMemoryPeakSize"
        static const char * const MaxSortMemoryPeakSize();       // "maxSortMemoryPeakSize"
        static const char * const MaxHashMemoryPeakSize();       // "maxHashMemoryPeakSize"
        static const char * const MaxExecutionMemoryPeakSize();  // "maxExecutionMemoryPeakSize"
        static const char * const MaxOverallReservedPeakSize();  // "maxOverallReservedPeakSize"
        static const char * const MaxWorkingSetPeakSize();       // "maxWorkingSetPeakSize"
        static const char * const MaxPrivateMemoryPeakSize();    // "maxPrivateMemoryPeakSize"
        static const char * const MaxManagedMemoryPeakSize();    // "maxManagedMemoryPeakSize"

        static const char * const AvgOverallMemoryPeakSize();    // "avgOverallMemoryPeakSize"
        static const char * const AvgGlobalMemoryPeakSize();     // "avgGlobalMemoryPeakSize"
        static const char * const AvgIOMemoryPeakSize();         // "avgIOMemoryPeakSize"
        static const char * const AvgSortMemoryPeakSize();       // "avgSortMemoryPeakSize"
        static const char * const AvgHashMemoryPeakSize();       // "avgHashMemoryPeakSize"
        static const char * const AvgExecutionMemoryPeakSize();  // "avgExecutionMemoryPeakSize"
        static const char * const AvgOverallReservedPeakSize();  // "avgOverallReservedPeakSize"
        static const char * const AvgWorkingSetPeakSize();       // "avgWorkingSetPeakSize"
        static const char * const AvgPrivateMemoryPeakSize();    // "avgPrivateMemoryPeakSize"
        static const char * const AvgManagedMemoryPeakSize();    // "avgManagedMemoryPeakSize"

// Hash Aggregates statistics
        static const char * const HashtableResetCount();         // "htResetCount"
        static const char * const HashtableMaxTotalMemory();     // "htMaxTotalMemory"
        static const char * const HashtableMaxDataMemory();      // "htMaxDataMemory"
        static const char * const HashtableInsertCount();        // "htInsertCount"
        static const char * const HashtableUpdateCount();        // "htUpdateCount"
        static const char * const HashtableLookupCount();        // "htLookupCount"
        static const char * const SpillProbeRowCount();          // "spProbeRowCount"
        static const char * const SpillBuildRowCount();          // "spBuildRowCount"
        static const char * const SpillMaxLevel();               // "spMaxLevel"

// Spool statistics
        static const char * const MaxBuffersProducedCount();     // "maxBuffersProducedCount"
        static const char * const AvgBuffersProducedCount();     // "avgBuffersProducedCount"
        static const char * const MaxBuffersSpilledCount();      // "maxBuffersSpilledCount"
        static const char * const AvgBuffersSpilledCount();      // "avgBuffersSpilledCount"
        static const char * const MaxBuffersLoadedCount();       // "maxBuffersLoadedCount"
        static const char * const AvgBuffersLoadedCount();       // "avgBuffersLoadedCount"

        static void WriteRowCount(TreeNode & node, const LONGLONG count);
        static void WriteKeyCount(TreeNode & node, const LONGLONG count);

        static void WriteIOStats(TreeNode & node, const Scanner::Statistics & ioStats);
        static void WriteCacheStats(TreeNode & node, const SSLibV3::DataUnitScanner::CacheStatistics & cacheStats);
        static void WriteMemoryStats(TreeNode & node, const SSLibV3::DataUnitScanner::MemoryStatistics & memoryStats);
    };

#pragma warning( push )
#pragma warning( disable : 4251 )

    //
    // Execution-time parameter value
    //
    struct SCOPE_ENGINE_API ParameterValue
    {
        std::string m_name;
        std::string m_type;
        std::shared_ptr<std::string> m_value; // UTF-8 encoded

        ParameterValue(const std::string & name, const std::string & type, std::shared_ptr<std::string> value) :
            m_name(name),
            m_type(type),
            m_value(value)
        {
        }

        ParameterValue(ParameterValue && other) :
            m_name(std::move(other.m_name)),
            m_type(std::move(other.m_type)),
            m_value(other.m_value)
        {
        }
    };

    typedef void (*ReportVertexStatusFunc)(void*);

    struct SCOPE_ENGINE_API VertexExecutionInfo
    {
        TreeNode & m_statsRoot;
        const std::vector<ParameterValue> m_parameters;
        UINT m_vertexId;
        void * m_scopeCEPCheckpointManager;
        ReportVertexStatusFunc m_reportStatusFunc;
        void * m_statusReportContext;

        VertexExecutionInfo(TreeNode & statsRoot) : m_statsRoot(statsRoot), m_vertexId(0), m_scopeCEPCheckpointManager(nullptr), m_reportStatusFunc(NULL), m_statusReportContext(NULL)
        {
        }

        VertexExecutionInfo(TreeNode & statsRoot, const std::vector<ParameterValue> & parameters) : m_statsRoot(statsRoot), m_parameters(parameters), 
            m_scopeCEPCheckpointManager(nullptr), m_vertexId(0), m_reportStatusFunc(nullptr), m_statusReportContext(nullptr)
        {
        }

    private:
        VertexExecutionInfo(const VertexExecutionInfo &);
        VertexExecutionInfo & operator=(const VertexExecutionInfo &);
    };

    // Input file information for extractor
    struct SCOPE_ENGINE_API InputFileInfo
    {
        std::string  inputFileName;
        UINT64       inputFileLength;
        bool         inputCrossSplit;   // flag passed from JM, to identify whether JM split stream at arbitrary offset
        UINT64       splitStartOffset;  // split's global start offet in stream
        UINT64       splitEndOffset;    // split's glboal end offset in stream

        InputFileInfo()
        {
            Init("");
        }
        
        InputFileInfo(const std::string& fileName)
        {            
            Init(fileName);
        }

    private:

        void Init(const std::string& fileName)
        {
            inputFileName    = fileName;
            inputFileLength  = 0;
            inputCrossSplit  = false; 
            splitStartOffset = 0;
            splitEndOffset   = 0;        
        }
    };

#pragma warning( pop )

    class SCOPE_ENGINE_API ScopeRandom
    {
        CsRandom* m_random;
        //disable copy constructor and assignment
        ScopeRandom(const ScopeRandom& other);
        ScopeRandom& ScopeRandom::operator = (const ScopeRandom& other);

        void Initialize(int seed);

    public:
        ScopeRandom();
        ScopeRandom(int seed);
        ~ScopeRandom();

        int Next();
    };

    // Type of execution environment
    enum EnvironmentType
    {
        // Uninitialized
        ET_None,

        // Vertex is executed Cosmos cluster
        ET_Cosmos,

        // Vertex is executed Cosmos cluster and allows to create rows up to 512Mb
        ET_CosmosBigRow,

        // Vertex is executed Azure cluster
        ET_Azure
    };

    class SCOPE_ENGINE_API Configuration
    {
    protected:
        EnvironmentType m_type;
        SIZE_T m_maxStringSize;
        SIZE_T m_maxBinarySize;
        SIZE_T m_maxVariableColumnSize;
        SIZE_T m_maxKeySize;
        SIZE_T m_maxInMemoryRowSize;
        SIZE_T m_maxOnDiskRowSize;

        Configuration() 
            : m_type(ET_None), m_maxStringSize(0), m_maxBinarySize(0), m_maxVariableColumnSize(0),
            m_maxKeySize(0), m_maxInMemoryRowSize(0), m_maxOnDiskRowSize(0)
        {
        }

    public:
        EnvironmentType GetEnvironmentType() const
        {
            return m_type;
        }

        SIZE_T GetMaxStringSize() const
        {
            return m_maxStringSize;
        }

        SIZE_T GetMaxBinarySize() const
        {
            return m_maxBinarySize;
        }

        SIZE_T GetMaxVariableColumnSize() const
        {
            return m_maxVariableColumnSize;
        }

        SIZE_T GetMaxKeySize() const
        {
            return m_maxBinarySize;
        }

        SIZE_T GetMaxInMemoryRowSize() const
        {
            return m_maxInMemoryRowSize;
        }

        SIZE_T GetMaxOnDiskRowSize() const
        {
            return m_maxOnDiskRowSize;
        }

        static const Configuration& Create(EnvironmentType type);
        static const Configuration& GetGlobal();
    };

    struct SCOPE_ENGINE_API SpookyHash
    {
        static void Hash128(const void *message, SIZE_T length, UINT64 *hash1, UINT64 *hash2);
        static UINT64 Hash64(const void *message, SIZE_T length, UINT64 seed);
        static UINT Hash32(const void *message, SIZE_T length, UINT seed);
    };
};
