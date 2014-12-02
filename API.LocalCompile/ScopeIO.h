#pragma once

#include <codecvt>
#include <ctime>

#include "ScopeContainers.h"
#include "ScopeUtils.h"
#include "ScopeDateTime.h"

#include <iostream>
#include <fstream>

namespace ScopeEngine
{
    static const char x_r = '\r';
    static const char x_n = '\n';
    static const char x_quote = '\"';
    static const char x_hash = '#';
    static const char x_pairquote [] = {'\"', '\"'};    
    static const char x_newline [] = {x_r, x_n};
    static const char x_null [] = {x_hash,'N','U','L','L',x_hash};
    static const char x_error [] = {x_hash,'E','R','R','O','R',x_hash};
    static const char x_tab [] = {x_hash,'T','A','B',x_hash};
    static const char x_escR [] = {x_hash,'R',x_hash};
    static const char x_escN [] = {x_hash,'N',x_hash};
    static const char x_escHash [] = {x_hash,'H','A','S','H',x_hash};
    static const char x_True [] = {'T','r','u','e'};
    static const char x_False [] = {'F','a','l','s','e'};
    static const char x_NaN [] = {'N','a','N'};
    static const char x_PositiveInf [] = {'I','n','f','i','n','i','t','y'};
    static const char x_NegativeInf [] = {'-','I','n','f','i','n','i','t','y'};

    static const char x_UTF7DecoderArray [] = {
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
        -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
        -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
    };


    static const bool x_UTF7DirectChars [] = {
        false, false, false, false, false, false, false, false, false, true, true, false, false, true, false, false, 
        false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, 
        true, false, false, false, false, false, false, true, true, true, false, false, true, true, true, true, 
        true, true, true, true, true, true, true, true, true, true, true, false, false, false, false, true, 
        false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, 
        true, true, true, true, true, true, true, true, true, true, true, false, false, false, false, false, 
        false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, 
        true, true, true, true, true, true, true, true, true, true, true, false, false, false, false, false
    };

    static const char x_Base64Chars [] = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
        'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',
        'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
        't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', '+', '/'
    };

#if defined(SCOPE_RUNTIME_EXPORT_DLL) || defined(SCOPE_RUNTIME_IMPORT_DLL)
    template class SCOPE_RUNTIME_API unique_ptr<Scanner, ScannerDeleter>;
#endif

    // A wrapper for cosmos input device. 
    // It handles the interaction with scopeengin IO system and provide a GetNextPage interface to InputStream
    class SCOPE_RUNTIME_API CosmosInput : public ExecutionStats
    {
        BlockDevice *       m_device;      // device object
        unique_ptr<Scanner, ScannerDeleter> m_scanner;     // scanner class which provide page level read and prefetch

        SIZE_T              m_ioBufSize;
        int                 m_ioBufCount;

        Scanner::Statistics m_statistics;

        bool                m_started;

        char *  m_buffer; // current reading start point
        SIZE_T  m_numRemain; // number of character remain in the buffer
        SIZE_T  m_position;
        SIZE_T  m_posInBuf;
        SIZE_T  m_posNextBuffer;
        SIZE_T  m_currentBufSize;
        std::string       m_recoverState; // cached state from load checkpoint

        void CloseScanner()
        {
            if (m_scanner->IsOpened())
            {
                if (m_started)
                {
                    m_started = false;
                    m_scanner->Finish();
                }

                m_scanner->Close();
            }
        }

        void ResetBuffer()
        {
            m_posNextBuffer = 0;
            m_currentBufSize = 0;
            m_numRemain = 0;
            m_posInBuf = 0;
            m_buffer = nullptr;
        }
        
    public:
        CosmosInput() : 
            m_device(nullptr), 
            m_ioBufSize(IOManager::x_defaultInputBufSize),
            m_ioBufCount(IOManager::x_defaultInputBufCount),
            m_started(false)
        {
        }

        CosmosInput(const std::string & filename, SIZE_T bufSize = IOManager::x_defaultInputBufSize, int bufCount = IOManager::x_defaultInputBufCount) :
            m_device(IOManager::GetGlobal()->GetDevice(filename)), 
            m_ioBufSize(bufSize),
            m_ioBufCount(bufCount),
            m_started(false)
        {
        }

        CosmosInput(BlockDevice* device, SIZE_T bufSize = IOManager::x_defaultInputBufSize, int bufCount = IOManager::x_defaultInputBufCount) :
            m_device(device), 
            m_ioBufSize(bufSize),
            m_ioBufCount(bufCount),
            m_started(false)
        {
        }

        void Init(bool startReadAhead = true, bool cancelable = false)
        {
            AutoExecStats stats(this);

            if (!m_device)
                throw ScopeStreamException(ScopeStreamException::BadDevice);

            //setup scanner and start scanning. 
            m_scanner.reset(Scanner::CreateScanner(m_device, MemoryManager::GetGlobal(), Scanner::STYPE_ReadOnly, m_ioBufSize, m_ioBufSize, m_ioBufCount));

            if (m_recoverState.size() != 0)
            {
                m_scanner->LoadState(m_recoverState);
                m_recoverState.clear();
            }

            m_scanner->Open(startReadAhead, cancelable);

            m_posNextBuffer = 0;
            m_currentBufSize = 0;
            m_numRemain = 0;
            m_position = 0;
            ResetBuffer();
        }

        void Close()
        {
            AutoExecStats stats(this);

            if (m_scanner)
            {
                CloseScanner();

                // Track statistics before scanner is destroyed.
                m_statistics = m_scanner->GetStatistics();
                m_statistics.ConvertToMilliSeconds();

                m_scanner.reset();
            }
        }

        // Jump forward specified amount of bytes
        void Skip(SIZE_T size)
        {
            while(m_numRemain < size)
            {
                size -= m_numRemain;
                m_position += m_numRemain;
                m_posInBuf += m_numRemain;
                m_numRemain = 0;

                // No more data to read return failure.
                if (!Refill())
                {
                    throw ScopeStreamException(ScopeStreamException::BadFormat);
                }
            }

            m_numRemain -= size;
            m_posInBuf += size;
            m_position += size;
        }

        SIZE_T GetCurrentPosition() const
        {
            return m_position;
        }
        // refill the input buffer.
        // For memory stream, we will simply return false.
        // For cosmos input, we will get next buffer from device.
        FORCE_INLINE bool Refill()
        {
            SIZE_T nRead=0;

            m_buffer = GetNextPage(nRead);
            if(m_buffer == nullptr || nRead == 0)
            {
                m_buffer = nullptr;
                return false;
            }
            m_numRemain = nRead;
            m_posInBuf = 0;
            return true;
        }

        // Read "size" of byte into dest buffer, return how many bytes were read
        unsigned int Read(char* dest, unsigned int size)
        {
            unsigned int bytesRead = 0;

            if (m_numRemain < size)
            {
                // Copy the remaining first, then fetch the next batch
                if (m_numRemain)
                {
                    memcpy(dest, m_buffer + m_posInBuf, m_numRemain);
                }

                bytesRead = static_cast<unsigned int>(m_numRemain);
                m_position += m_numRemain;
                m_posInBuf += m_numRemain;
                m_numRemain = 0;

                if (!Refill())
                {
                    return bytesRead;
                }
                else
                {
                    bytesRead += Read(dest + bytesRead, size - bytesRead);
                    return bytesRead;
                }
            }
            else
            {
                memcpy(dest, m_buffer + m_posInBuf, size);
                m_position += size;
                m_posInBuf += size;
                m_numRemain -= size;
                return size;
            }
        }

        // Reach one page from scanner. The page size is provided by IO system to
        // get best performance.
        char * GetNextPage(SIZE_T & numRead, UINT64 initialOffset = 0)
        {
            AutoExecStats stats(this);

            if (!m_started)
            {
                m_scanner->Start(initialOffset);
                m_started = true;
                m_position = initialOffset;
                m_posNextBuffer = initialOffset;
            }

            const BufferDescriptor* buffer;
            SIZE_T bufferSize;

            bool next = m_scanner->GetNext(buffer, bufferSize);
            if (next && bufferSize > 0)
            {
                numRead = m_numRemain = bufferSize;
                stats.IncreaseRowCount(bufferSize);
                m_buffer = (char *)(buffer->m_buffer);
                m_posNextBuffer += bufferSize;
                m_currentBufSize = bufferSize;
            }
            else
            {
                ResetBuffer();
                numRead = 0;
            }

            m_posInBuf = 0;
            return m_buffer;
        }

        SIZE_T Seek(SIZE_T position)
        {
            if (m_buffer != nullptr && position >= m_posNextBuffer - m_currentBufSize && position <= m_posNextBuffer)
            {
                m_position = position;
                m_numRemain = m_posNextBuffer - position;
                m_posInBuf = m_currentBufSize - m_numRemain;
            }
            else
            {
                Close();
                Init(false);
                SIZE_T read;
                GetNextPage(read, position);
            }
            return m_position;
        }

        UINT64 Length()
        {
            AutoExecStats stats(this);

            SCOPE_ASSERT(m_scanner && m_scanner->IsOpened());

            return m_scanner->Length();
        }

        // Return the filename associated with this stream
        std::string StreamName() const
        {
            return m_scanner->GetStreamName();
        }

        // Restart the input stream. Next GetNextPage will start from position of initialOffset.
        // There is no read ahead for the first page as caller will call GetNextPage immediately. 
        void Restart()
        {
            AutoExecStats stats(this);

            if (m_scanner)
            {
                CloseScanner();

                // open and start from offset 0 again.
                m_scanner->Open(true);
                m_scanner->Start(0);
            }
            
            m_position = 0;
            ResetBuffer();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            RuntimeStats::WriteIOStats(root, m_statistics);
            auto & node = root.AddElement("IOBuffers");
            node.AddAttribute(RuntimeStats::MaxBufferCount(), m_ioBufCount);
            node.AddAttribute(RuntimeStats::MaxBufferSize(), m_ioBufSize);
            node.AddAttribute(RuntimeStats::MaxBufferMemory(), m_statistics.m_memorySize);
            node.AddAttribute(RuntimeStats::AvgBufferMemory(), m_statistics.m_memorySize);
        }

        void SaveState(BinaryOutputStream& output, UINT64 position);
        void LoadState(BinaryInputStream& input);
        void ClearState();
    };

    //
    // Wraps managed Stream to cache UDT (de)serialization
    //
    class SCOPE_RUNTIME_API StreamWithManagedWrapper
    {
#if !defined(SCOPE_NO_UDT)
        unique_ptr<ScopeManagedHandle> m_wrapper; // managed wrapper used to serialize UDT (lazily constructed)

    public:

        // Access to the managed wrapper for a standalone SerializeUDT function
        unique_ptr<ScopeManagedHandle>& Wrapper()
        {
            return m_wrapper;
        }
#endif // SCOPE_NO_UDT
    };

    // A wrapper to provide stream interface on top of cosmos input which provide block reading interface.
    // It hides the page from the reader.
    template<typename InputType>
    class SCOPE_RUNTIME_API BinaryInputStreamBase : public StreamWithManagedWrapper
    {
        InputType  m_asyncInput;       // blocked cosmos input stream 
        IncrementalAllocator  * m_allocator;      // memory allocator pointer for deserialization

        unsigned int DecodeLength()
        {
            // Decode the variable length encoded string length first
            unsigned int len = 0;
            unsigned int shift = 7;
            char b = 0;
            Read(b);
            len = b & 0x7f;

            while(b & 0x80)
            {
                Read(b);
                len |= (b & 0x7f) << shift;
                shift += 7;
            }

            return len;
        }

    protected:
        IncrementalAllocator* GetAllocator()
        {
            SCOPE_ASSERT(m_allocator != NULL);
            return m_allocator;
        }

    public:
        // constructor - until we get variadic templates
        template<typename Arg1>
        BinaryInputStreamBase(IncrementalAllocator * allocator, const Arg1& arg1) :
            m_asyncInput(arg1), 
            m_allocator(allocator)
        {
        }

        template<typename Arg1, typename Arg2>
        BinaryInputStreamBase(IncrementalAllocator * allocator, const Arg1& arg1, const Arg2& arg2) :
            m_asyncInput(arg1, arg2), 
            m_allocator(allocator)
        {
        }

        template<typename Arg1, typename Arg2, typename Arg3>
        BinaryInputStreamBase(IncrementalAllocator * allocator, const Arg1& arg1, const Arg2& arg2, const Arg3& arg3) :
            m_asyncInput(arg1, arg2, arg3), 
            m_allocator(allocator)
        {
        }

        SIZE_T Position() const
        {
            return m_asyncInput.GetCurrentPosition();
        }

        // Init 
        void Init(bool readAhead = true)
        {
            m_asyncInput.Init(readAhead);
        }

        // close stream
        void Close()
        {
            m_asyncInput.Close();
        }

        // deserialize a scope supported type written by BinaryWriter
        template<typename T>
        void Read(T & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(T)) != sizeof(T))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        unsigned int Read(char * dest, unsigned int size)
        {
            return m_asyncInput.Read(dest, size);
        }

        // Fill FString/FBinary with size
        template<typename T>
        bool ReadFixedArray(FixedArrayType<T> & s, unsigned int size)
        {
            unsigned int bytesRead = 0;
            if (size == 0)
            {
                s.SetEmpty();
            }
            else
            {
                // @TODO: Initially, due to performance concerns fixed array pointer (size) is not saved for validation
                // We should try to change this in the future and make sure that we never write past size of the buffer.
                char * dest = (char *) s.Reserve(size, m_allocator);
                bytesRead = Read(dest, size);
            }

            return bytesRead == size;
        }

        // deserialize a string object written by BinaryWriter
        void Read(FString & str)
        {
            unsigned int len = DecodeLength();

            // Fill the FString
            if (!ReadFixedArray(str, len))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        // deserialize a binary object written by BinaryWriter
        void Read(FBinary & bin)
        {
            unsigned int len = DecodeLength();

            // Fill the FBinary
            if (!ReadFixedArray(bin, len))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        // deserialize a scope map
        template<typename K, typename V>
        void Read(ScopeMapNative<K,V> & m)
        {
            m.Deserialize(this, m_allocator);
        }

        // deserialize a scope array
        template<typename T>
        void Read(ScopeArrayNative<T> & s)
        {
            s.Deserialize(this, m_allocator);
        }

        // deserialize a scope supported type written by BiniaryWriter
        void Read(ScopeDateTime & s)
        {
            __int64 binaryTime;
            Read(binaryTime);
            s = ScopeDateTime::FromBinary(binaryTime);
        }

        // deserialize a Decimal type written by BiniaryWriter
        void Read(ScopeDecimal & s)
        {
            Read(s.Lo32Bit());
            Read(s.Mid32Bit());
            Read(s.Hi32Bit());
            Read(s.SignScale32Bit());
            SCOPE_ASSERT(s.IsValid());
        }

        // deserialize a scope supported type written by BinaryWriter
        template<typename T>
        void Read(NativeNullable<T> & s)
        {
            Read(s.get());
            s.ClearNull();
        }

        void Read(__int64& i)
        {
            Read<__int64>(i);
        }

        void Read(bool& i)
        {
            Read<bool>(i);
        }

        void Read(char& i)
        {
            Read<char>(i);
        }

        void Read(wchar_t& i)
        {
            Read<wchar_t>(i);
        }

        void Read(int& i)
        {
            Read<int>(i);
        }

#if !defined(SCOPE_NO_UDT)
        // deserialize a UDT type written by BinaryWriter
        template<int ColumnTypeID>
        void Read(ScopeUDTColumnTypeStatic<ColumnTypeID> & s)
        {
            DeserializeUDT(s, this);
        }

        // deserialize a UDT type written by BinaryWriter
        void Read(ScopeUDTColumnTypeDynamic & s)
        {
            DeserializeUDT(s, this);
        }
#endif // SCOPE_NO_UDT

        PartitionMetadata * ReadMetadata()
        {
            return nullptr;
        }

        void ReadIndexedPartitionMetadata(std::vector<std::shared_ptr<PartitionMetadata>>& partitionMetadataList)
        {
        }

        void DiscardMetadata()
        {
        }

        void DiscardIndexedPartitionMetadata()
        {
        }

        // Rewind the input stream
        void ReWind()
        {
            //Start to read from beginning again.
            m_asyncInput.Restart();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_asyncInput.WriteRuntimeStats(root);
        }

        // return Inclusive time of input stream
        LONGLONG GetInclusiveTimeMillisecond()
        {
            return m_asyncInput.GetInclusiveTimeMillisecond();
        }

        InputType& GetInputer()
        {
            return m_asyncInput;
        }
    };

    class SCOPE_RUNTIME_API BinaryInputStream : public BinaryInputStreamBase<CosmosInput>
    {
        InputFileInfo   m_input;         // input file info
        
    public:
    
        BinaryInputStream(const InputFileInfo & input, IncrementalAllocator * allocator, SIZE_T bufSize, int bufCount) :
            BinaryInputStreamBase(allocator, input.inputFileName, bufSize, bufCount),
            m_input(input)
        {
            // Check split start/end offset
            if (m_input.inputCrossSplit)
            {
                SCOPE_ASSERT(m_input.splitEndOffset >= m_input.splitStartOffset);
                SCOPE_ASSERT(m_input.inputFileLength >= m_input.splitEndOffset - m_input.splitStartOffset);
            }        
        }
        
    };

    class MemoryInput
    {
        char* m_buffer;
        unsigned int m_size;
        unsigned int  m_position;
        bool m_eos;
    public:
        MemoryInput(char* buffer, SIZE_T size) : m_buffer(buffer), m_size(static_cast<unsigned int>(size)), m_eos(false), m_position(0)
        {
        }

        void Init(bool startReadAhead = true)
        {
            UNREFERENCED_PARAMETER(startReadAhead);

            m_eos = false;
        }

        void Close()
        {
        }

        // Read "size" of byte into dest buffer, return how many bytes were read
        unsigned int Read(char* dest, unsigned int size)
        {
            m_eos = size > m_size - m_position;
            int byteToRead = m_eos ? (m_size - m_position) : size;
            memcpy(dest, m_buffer + m_position, byteToRead);
            m_position += byteToRead;
            return byteToRead; 
        }

        void Restart()
        {
            m_eos = false;
            m_position = 0;
        }

        SIZE_T GetCurrentPosition() const
        {
            return static_cast<SIZE_T>(m_position);
        }

        void WriteRuntimeStats(TreeNode &)
        {
            // No stats
        }

        // return Inclusive time of input stream
        LONGLONG GetInclusiveTimeMillisecond()
        {
            return 0;
        }

        void SaveState(BinaryOutputStream&, UINT64)
        {
        }

        void LoadState(BinaryInputStream&)
        {
        }
    };

    class SCOPE_RUNTIME_API MemoryInputStream : public BinaryInputStreamBase<MemoryInput>
    {
    public:
        MemoryInputStream(IncrementalAllocator * allocator, char* buffer, SIZE_T size) : BinaryInputStreamBase(allocator, buffer, size)
        {
        }
    };

    class ResourceInput
    {
        std::ifstream m_stream;
        size_t        m_size;
        size_t        m_position;
        bool          m_eos;

    public:
        ResourceInput(const std::string & filename) : m_stream(), m_position(0), m_eos(false)
        {
            try
            {
                m_stream.exceptions(ifstream::failbit | ifstream::badbit);
                m_stream.open(filename, ios::binary | ios::in);

                // Get file size
                m_stream.seekg(0, ios::end);
                m_size = m_stream.tellg();
                m_stream.seekg(0, ios::beg);
            }
            catch (istream::failure e)
            {
                throw new RuntimeException(E_SYSTEM_RESOURCE_OPEN, filename);
            }
        }

        void Init(bool startReadAhead = true)
        {
            UNREFERENCED_PARAMETER(startReadAhead);
        }

        void Close()
        {
            m_stream.close();
        }

        // Read "size" of byte into dest buffer, return how many bytes were read
        unsigned int Read(char* dest, unsigned int size)
        {
            try
            {
                m_eos = size > (m_size - m_position);
                size_t byteToRead = m_eos ? (m_size - m_position) : size;
                m_stream.read(dest, byteToRead);
                m_position += byteToRead;
                return static_cast<int>(byteToRead);
            }
            catch (istream::failure e)
            {
                throw new RuntimeException(E_SYSTEM_RESOURCE_READ, e.what());
            }
        }

        void Restart()
        {
            m_stream.seekg(0, m_stream.beg);
            m_position = 0;
            m_eos = false;
        }

        SIZE_T GetCurrentPosition() const
        {
            return m_position;
        }

        void WriteRuntimeStats(TreeNode &)
        {
            // No stats
        }

        // return Inclusive time of input stream
        LONGLONG GetInclusiveTimeMillisecond()
        {
            return 0;
        }

        void SaveState(BinaryOutputStream&, UINT64)
        {
        }

        void LoadState(BinaryInputStream&)
        {
        }
    };

    class SCOPE_RUNTIME_API ResourceInputStream : public BinaryInputStreamBase<ResourceInput>
    {
    public:
        ResourceInputStream(IncrementalAllocator * allocator, const std::string & filename) : BinaryInputStreamBase(allocator, filename)
        {
        }
    };

    //
    // Binary input stream with payload
    //
    template<typename Payload>
    class AugmentedBinaryInputStream : public BinaryInputStream
    {
    public:
        AugmentedBinaryInputStream(const InputFileInfo & input, IncrementalAllocator * allocator, SIZE_T bufSize, int bufCount) :
            BinaryInputStream(input, allocator, bufSize, bufCount)
        {
        }

        PartitionMetadata * ReadMetadata()
        {
            cout << "Reading metadata" << endl;
            return new Payload(this);
        }

        void DiscardMetadata()
        {
            cout << "Discarding metadata" << endl;
            Payload::Discard(this);
        }

        void ReadIndexedPartitionMetadata(std::vector<std::shared_ptr<PartitionMetadata>>& PartitionMetadataList)
        {
            cout << "Reading indexed partition metadata" << endl;
            __int64 count = 0;
            Read(count);
            SCOPE_ASSERT(count > 0);
            PartitionMetadataList.resize(count);
            for(__int64 i = 0; i < count; i++)
            {
                PartitionMetadataList[i].reset(new Payload(this, GetAllocator()));
            }
        }

        void DiscardIndexedPartitionMetadata()
        {
            cout << "Discarding indexed partition metadata" << endl;
            __int64 count = 0;
            Read(count);
            SCOPE_ASSERT(count > 0);
            for(__int64 i = 0; i < count; i++)
            {
                Payload::Discard(this);
            }
        }
    };

    template<typename Schema, typename PolicyType, typename MetadataSchema, int MetadataId>
    class SStreamOutputStream : public ExecutionStats
    {
        static const int PARTITION_NOT_EXIST = -2;

        std::string     m_filename;
        SIZE_T          m_bufSize;
        int             m_bufCount;     // UNDONE: Have the outputer honor this bufCount.

        SSLibV3::DataUnitDescriptor m_dataUnitDesc;
        BlockDevice*    m_device;           // non-owning, owned by IOManager
        MemoryManager*     m_memMgr;

        unique_ptr<Scanner, ScannerDeleter> m_dataScanner;
        unique_ptr<SSLibV3::DataUnitWriter> m_dataUnit_ptr;

        GUID                m_affinityId;
        UINT64              m_partitionIndex;
        int                 m_columngroupIndex;

        std::shared_ptr<SSLibV3::SStreamStatistics> m_statistics;

        std::string     m_partitionKeyRangeSerialized;

        std::string     m_dataSchemaString;
        std::string     m_indexSchemaString;
        bool            m_preferSSD;
        bool            m_enableBloomFilter;

    protected:

    public:
        SStreamOutputStream(std::string& filename, int columngroupIndex, SIZE_T bufSize, int bufCnt, bool preferSSD, bool enableBloomFilter) :
            m_filename(filename),
            m_bufSize(bufSize),
            m_bufCount(bufCnt),
            // partition index will be initialized to correct value in GetPartitionInfo
            // MetadataId == -1 means that it doesn't need metadata
            // In the splitter operator, MetadataId is -1 if the partition type is NONE
            // and it won't call GetPartitionInfo to initialize the partition index
            // so we set the default value to 0 if MetadataId is -1
            m_partitionIndex(MetadataId != -1 ? (UINT64)-1 : 0),
            m_columngroupIndex(columngroupIndex),
            m_preferSSD(preferSSD),
            m_enableBloomFilter(enableBloomFilter),
            m_device(NULL),
            m_memMgr(NULL)
        {
        }

        void Init()
        {
            Init(Scanner::STYPE_Create);
        }

        void Init(Scanner::ScannerType dataScannerType, UINT32 bloomFilterBitCount = BloomFilter::DefaultBitCount)
        {
            AutoExecStats stats(this);
            // prepare the descriptor
            m_dataUnitDesc.m_dataColumnSizes = PolicyType::m_dataColumnSizes;
            m_dataUnitDesc.m_dataColumnCnt = array_size(PolicyType::m_dataColumnSizes);
            m_dataUnitDesc.m_indexColumnSizes = PolicyType::m_indexColumnSizes;
            m_dataUnitDesc.m_indexColumnCnt = array_size(PolicyType::m_indexColumnSizes);
            m_dataUnitDesc.m_sortKeys = PolicyType::m_dataPageSortKeys;
            m_dataUnitDesc.m_sortKeysCnt = PolicyType::m_dataPageSortKeysCnt;
            m_dataUnitDesc.m_descending = false; // doesn't matter for writer.
            m_dataUnitDesc.m_blockSize  = PolicyType::m_blockSize;
            m_dataUnitDesc.m_numBloomFilterKeys = m_enableBloomFilter? (BYTE)m_dataUnitDesc.m_sortKeysCnt : (BYTE)0;

            // hardcode for now
            m_dataUnitDesc.m_numOfBuffers = m_bufCount; 

            m_device = ScopeEngine::IOManager::GetGlobal()->GetDevice(m_filename);

            m_memMgr = MemoryManager::GetGlobal();

            m_dataScanner.reset(Scanner::CreateScanner(m_device, m_memMgr, dataScannerType, Configuration::GetGlobal().GetMaxOnDiskRowSize(), m_dataUnitDesc.m_blockSize, m_dataUnitDesc.m_numOfBuffers, m_preferSSD));

            m_dataScanner->Open();
            m_dataScanner->Start();

            m_dataUnit_ptr.reset(SSLibV3::DataUnitWriter::CreateWriter(m_dataScanner.get(), m_memMgr, bloomFilterBitCount));

            m_affinityId = m_dataScanner->InitializeStreamAffinity();

            m_statistics.reset(
                SSLibV3::SStreamStatistics::Create( PolicyType::m_columnNames, array_size(PolicyType::m_columnNames)));

            m_dataSchemaString = PolicyType::DataSchemaString();

            m_indexSchemaString = SSLibV3::GenerateIndexSchema(
                PolicyType::m_columnNames,
                PolicyType::m_columnTypes,
                array_size(PolicyType::m_columnNames),
                PolicyType::m_dataPageSortKeys,
                PolicyType::m_dataPageSortOrders,
                PolicyType::m_dataPageSortKeysCnt);

            // Alway use NULL for partitionMetadata when writing the partition. (column group)
            m_dataUnit_ptr->Open(m_dataUnitDesc, 
                &PolicyType::SerializeRow,
                m_dataSchemaString.c_str(),
                m_indexSchemaString.c_str(),
                m_affinityId, 
                m_statistics.get());
        }

        void GetPartitionInfo(PartitionMetadata* payload)
        {
            m_partitionIndex = 0;
            m_partitionKeyRangeSerialized.clear();

            // condition on template param.
            // PartitionKeyRange<MetadataSchema, MetadataId> is define only when there is a valid partition schema.
            if (MetadataId != -1)
            {
                typedef PartitionKeyRange<MetadataSchema, MetadataId> PartitionKeyRangeType;
                SCOPE_ASSERT( payload != nullptr);

                m_partitionIndex = payload->GetPartitionId();

                if (m_partitionIndex != PartitionMetadata::PARTITION_NOT_EXIST)
                {
                    MemoryOutputStream ostream;

                    PartitionKeyRangeType::SerializeForSS( &ostream, payload);

                    ostream.Flush();

                    auto buffer = (char*) ostream.GetOutputer().Buffer();
                    auto len = ostream.GetOutputer().Tellp();

                    m_partitionKeyRangeSerialized = string(buffer, len);
                }
            }
        }

        void AppendRow(Schema & output)
        {
            AutoExecStats stats(this);
            m_dataUnit_ptr->AppendRow(&output);
            stats.IncreaseRowCount(1);
        }

        void Flush()
        {
            m_dataUnit_ptr->Flush();
        }

        void Close()
        {
            AutoExecStats stats(this);

            if (ValidPartition())
            {
                m_dataUnit_ptr->Close();
            }

            // Finish and close the data scanner
            if (m_dataScanner->IsOpened())
            {
                m_dataScanner->Finish();
                m_dataScanner->Close();
            }
        }

        bool ValidPartition() const
        {
            return m_partitionIndex != PARTITION_NOT_EXIST;
        }

        UINT64 Length() const
        {
            return m_dataUnit_ptr->Length();
        }

        UINT64 DataLength() const
        {
            return m_dataUnit_ptr->DataLength();
        }

        UINT64 RowCount() const
        {
            return m_dataUnit_ptr->RowCount();
        }

        std::shared_ptr<SSLibV3::SStreamStatistics> GetStatistics() 
        {
            return m_statistics;
        }

        GUID GetAffinityGuid() const
        {
            return m_affinityId;
        }

        int GetPartitionIndex() const
        {
            int partitionIndex = (int)m_partitionIndex;
            SCOPE_ASSERT(partitionIndex >= 0);
            return partitionIndex;
        }

        string GetPartitionKeyRange() const
        {
            return m_partitionKeyRangeSerialized;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("SStreamOutputStream");
            if (m_dataScanner != NULL)
            {
                m_dataScanner->WriteRuntimeStats(node);
            }
            if (m_dataUnit_ptr != NULL)
            {
                m_dataUnit_ptr->WriteRuntimeStats(node);
            }
            node.AddAttribute(RuntimeStats::MaxPartitionKeyRangeSize(), m_partitionKeyRangeSerialized.size());
        }
    };


    // Set of template to handle different text encoding conversion from/to utf8 encoding
    template<TextEncoding encoding>
    class TextEncodingConverter
    {
        FORCE_INLINE static SIZE_T SafeCopy(    char * in, 
            char * inLast, 
            char * out, 
            char * outLast
            )
        {
            SIZE_T size = inLast - in;
            SIZE_T spaceRemain = outLast - out;

            SIZE_T nCopy = std::min(spaceRemain, size);

            //copy as much as possible to out buffer, 
            memcpy(out, in, nCopy);

            return nCopy;
        }

    public:
        TextEncodingConverter()
        {
        }

        // default implementation for ASCII, UTF8, and Default encoding
        void ConvertToUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            SIZE_T nCopy = SafeCopy(in, inLast, out, outLast);

            inMid = in + nCopy;
            outMid = out + nCopy;
        }

        // default implementation for ASCII, UTF8, and Default encoding
        INLINE static void ConvertFromUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            SIZE_T nCopy = SafeCopy(in, inLast, out, outLast);

            inMid = in + nCopy;
            outMid = out + nCopy;
        }
    };

    template<>
    class TextEncodingConverter<Unicode>
    {
        typedef std::codecvt_utf8_utf16<char16_t, 0x10ffff, std::little_endian> ConverterType;

        mbstate_t         m_state;
        ConverterType     m_conv;      // converts between UTF-8 <-> UTF-16

    public:
        TextEncodingConverter():m_state(0)
        {
        }

        void ConvertToUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            if (((inLast-in) & 0x1) != 0)
            {
                // if input data is not aligned properly, we will keep the remain during next round of conversion
                inLast = in + ((inLast-in) & (~(SIZE_T)0x1));
            }

            const char16_t * pInNext = NULL;

            int res = m_conv.out(m_state, 
                (const char16_t *)in, 
                (const char16_t *)inLast, 
                pInNext, 
                out, 
                outLast, 
                outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            inMid = (char *) pInNext;

        }

        void ConvertFromUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            const char * pInNext = NULL;

            int res = m_conv.in(m_state, 
                in, 
                inLast, 
                pInNext, 
                (char16_t *)out, 
                (char16_t *)outLast, 
                (char16_t *&)outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            if (((outMid-out) & 0x1) !=0)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            inMid = (char *)pInNext;
        }
    };

    template<>
    class TextEncodingConverter<BigEndianUnicode>
    {
        typedef std::codecvt_utf8_utf16<char16_t, 0x10ffff, std::little_endian> ConverterType;

        mbstate_t        m_state;
        ConverterType    m_conv;      // converts between UTF-8 <-> UTF-16

    public:
        TextEncodingConverter():m_state(0)
        {
        }

        void ConvertToUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            if (((inLast-in) & 0x1) != 0)
            {
                // if input data is not aligned properly, we will keep the remain during next round of conversion
                inLast = in + ((inLast-in) & (~(SIZE_T)0x1));
            }

            // Convert input to little endian first
            for(char * start = in; start < inLast; start+=sizeof(char16_t))
            {
                *(unsigned short *)start = ByteSwap(*(unsigned short *)start);
            }

            const char16_t * pInNext = NULL;

            int res = m_conv.out(m_state, 
                (const char16_t *)in, 
                (const char16_t *)inLast, 
                pInNext, 
                out, 
                outLast, 
                outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            inMid = (char *) pInNext;

        }

        void ConvertFromUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            const char * pInNext = NULL;

            int res = m_conv.in(m_state, 
                in, 
                inLast, 
                pInNext, 
                (char16_t *)out, 
                (char16_t *)outLast, 
                (char16_t *&)outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            if (((outMid-out) & 0x1) != 0)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            // Convert output to big endian 
            for(char * start = out; start < outMid; start+=sizeof(char16_t))
            {
                *(unsigned short *)start = ByteSwap(*(unsigned short *)start);
            }

            inMid = (char *) pInNext;
        }
    };

    template<>
    class TextEncodingConverter<UTF32>
    {
        typedef std::codecvt_utf8<char32_t, 0x10ffff, std::little_endian> ConverterType;


        mbstate_t        m_state;
        ConverterType    m_conv;      // converts between UTF-8 <-> UTF-16

    public:
        TextEncodingConverter():m_state(0)
        {
        }

        void ConvertToUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            if (((inLast-in) & 0x3) != 0)
            {
                // if input data is not aligned properly, we will keep the remain during next round of conversion
                inLast = in + ((inLast-in) & (~(SIZE_T)0x3));
            }

            const char32_t * pInNext = NULL;

            int res = m_conv.out(m_state, 
                (const char32_t *)in, 
                (const char32_t *)inLast, 
                pInNext, 
                out, 
                outLast, 
                outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            inMid = (char *) pInNext;

        }

        void ConvertFromUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            const char * pInNext = NULL;

            int res = m_conv.in(m_state, 
                in, 
                inLast, 
                pInNext, 
                (char32_t *)out, 
                (char32_t *)outLast, 
                (char32_t *&)outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            if (((outMid-out) & 0x3) != 0)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            inMid = (char *)pInNext;
        }
    };

    template<>
    class TextEncodingConverter<BigEndianUTF32>
    {
        typedef std::codecvt_utf8<char32_t, 0x10ffff, std::little_endian> ConverterType;

        mbstate_t        m_state;
        ConverterType    m_conv;      // converts between UTF-8 <-> UTF-16

    public:
        TextEncodingConverter():m_state(0)
        {
        }

        void ConvertToUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            if (((inLast-in) & 0x3) != 0)
            {
                // if input data is not aligned properly, we will keep the remain during next round of conversion
                inLast = in + ((inLast-in) & (~(SIZE_T)0x3));
            }

            // Convert input to little endian first
            for(char * start = in; start < inLast; start+=sizeof(char32_t))
            {
                *(ULONG*)start = ByteSwap(*(ULONG*)start);
            }

            const char32_t * pInNext = NULL;

            int res = m_conv.out(m_state, 
                (const char32_t *)in, 
                (const char32_t *)inLast, 
                pInNext, 
                out, 
                outLast, 
                outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            inMid = (char *) pInNext;

        }

        void ConvertFromUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            const char * pInNext = NULL;

            int res = m_conv.in(m_state, 
                in, 
                inLast, 
                pInNext, 
                (char32_t *)out, 
                (char32_t *)outLast, 
                (char32_t *&)outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            if (((outMid-out) & 0x3) != 0)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            // Convert output to big endian 
            for(char * start = out; start < outMid; start+=sizeof(char32_t))
            {
                *(ULONG*)start = ByteSwap(*(ULONG*)start);
            }

            inMid = (char *) pInNext;
        }
    };

    template<>
    class TextEncodingConverter<UTF7>
    {
        // Variable to keep track of conversion state between different buffers.
        int     m_bits;
        int     m_bitCount;
        bool    m_firstByte;
        int     m_savedCh;

        FORCE_INLINE static bool OutputUtf8Char(int c, char *& outMid, char * outLast)
        {
            // Output character in UTF8 encoding
            if (c < 0x80)
            {
                if (outMid < outLast)
                    *outMid++ = (char)c;
                else
                    return false;
            }
            else if (c < 0x800)
            {
                if (outMid + 1 < outLast)
                {
                    *outMid++ = (char)(0xc0 | (c >> 6));
                    *outMid++ = (char)(0x80 | (c & 0x3f));
                }
                else
                    return false;
            }
            else if (c < 0x10000)
            {
                if (outMid + 2 < outLast)
                {
                    *outMid++ = (char)(0xe0 | (c >> 12));
                    *outMid++ = (char)(0x80 | ((c >> 6) & 0x3f));
                    *outMid++ = (char)(0x80 | (c & 0x3f));
                }
                else 
                    return false;
            }
            else
            {
                if (outMid + 3 < outLast)
                {
                    *outMid++ = (char)(0xf0 | (c >> 18));
                    *outMid++ = (char)(0x80 | ((c >> 12) & 0x3f));
                    *outMid++ = (char)(0x80 | ((c >> 6) & 0x3f));
                    *outMid++ = (char)(0x80 | (c & 0x3f));
                }
                else 
                    return false;
            }

            return true;
        }

    public:
        TextEncodingConverter():m_bits(0), m_bitCount(-1), m_firstByte(false), m_savedCh(-1)
        {
        }

        void ConvertToUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            outMid = out;
            inMid = in;

            if (m_savedCh >= 0)
            {
                // Output character in UTF8 encoding
                if (!OutputUtf8Char(m_savedCh, outMid, outLast))
                {
                    return;
                }

                m_savedCh = -1;
            }

            // We may have had bits in the decoder that we couldn't output last time, so do so now
            if (m_bitCount >= 16)
            {
                // Check our decoder buffer
                int c = (m_bits >> (m_bitCount - 16)) & 0xFFFF;

                // Output character in UTF8 encoding
                if (!OutputUtf8Char(c, outMid, outLast))
                {
                    return;
                }

                // Used this one, clean up extra bits
                m_bitCount -= 16;
            }

            // Loop through the input
            for ( ; inMid < inLast ; inMid++)
            {
                char currentByte = *inMid;
                int c = 0;

                if (m_bitCount >= 0)
                {
                    //
                    // Modified base 64 encoding.
                    //
                    char v;
                    if (currentByte < 0x80 && ((v = x_UTF7DecoderArray[currentByte]) >=0))
                    {
                        m_firstByte = false;
                        m_bits = (m_bits << 6) | v;
                        m_bitCount += 6;
                        if (m_bitCount >= 16)
                        {
                            c = (m_bits >> (m_bitCount - 16)) & 0xFFFF;
                            m_bitCount -= 16;
                        }
                        else
                        {
                            // If not enough bits just continue
                            continue;
                        }
                    }
                    else
                    {
                        // If it wasn't a base 64 byte, everything's going to turn off base 64 mode
                        m_bitCount = -1;

                        if (currentByte != '-')
                        {
                            // it must be >= 0x80 (because of 1st if statemtn)
                            // We need this check since the base64Values[b] check below need b <= 0x7f.
                            // This is not a valid base 64 byte.  Terminate the shifted-sequence and
                            // emit this byte.

                            // not in base 64 table
                            // According to the RFC 1642 and the example code of UTF-7
                            // in Unicode 2.0, we should just zero-extend the invalid UTF7 byte

                            // We don't support fallback. .
                            continue;
                        }

                        //
                        // The encoding for '+' is "+-".
                        //
                        if (m_firstByte) 
                        {
                            c = '+';
                        }
                        else
                        {
                            // We just turn it off if not emitting a +, so we're done.
                            continue;
                        }
                    }
                }
                else if (currentByte == '+')
                {
                    //
                    // Found the start of a modified base 64 encoding block or a plus sign.
                    //
                    m_bitCount = 0;
                    m_firstByte = true;
                    continue;
                }
                else
                {
                    // Normal character
                    if (currentByte >= 0x80)
                    {
                        // We don't support fallback. So throw exception.
                        throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
                    }

                    // Use the normal character
                    c = currentByte;
                }

                if (c >= 0)
                {
                    // Output character in UTF8 encoding
                    if (!OutputUtf8Char(c, outMid, outLast))
                    {
                        // save c if we failed to output when there is not enough output buffer.
                        m_savedCh = c;
                        return;
                    }
                }
            }
        }

        //
        // Convert the data from UTF-8 to RFC 2060's UTF-7.
        void ConvertFromUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            SIZE_T u8len = inLast-in;

            inMid = in;
            outMid = out;

            // May have had too many left over
            while (m_bitCount >= 6)
            {
                // Try to add the next byte
                if (outMid < outLast)
                {
                    *outMid++ = x_Base64Chars[(m_bits >> (m_bitCount-6)) & 0x3F];
                }
                else
                {
                    return;
                }

                m_bitCount -= 6;
            }

            // save a check point for output  so that we can roll back if output buffer is not large enough.
            char * inCur = inMid;

            while(inCur < inLast)
            {
                unsigned char u8 = (unsigned char)(*inCur++);
                unsigned short ch; 
                int n = 0;

                if (u8 < 0x80)
                {
                    ch = u8;
                    n = 0;
                }
                else if (u8 < 0xc2)
                {
                    throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
                }
                else if (u8 < 0xe0)
                {
                    ch = u8 & 0x1f;
                    n = 1;
                }
                else if (u8 < 0xf0)
                {
                    ch = u8 & 0x0f;
                    n = 2;
                }
                else if (u8 < 0xf8)
                {
                    ch = u8 & 0x07;
                    n = 3;
                }
                else if (u8 < 0xfc)
                {
                    ch = u8 & 0x03;
                    n = 4;
                }
                else if (u8 < 0xfe)
                {
                    ch = u8 & 0x01;
                    n = 5;
                }
                else
                {
                    throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
                }

                u8len--;

                // not enough input stream, return
                if (n > u8len)
                {
                    return;
                }

                for (int j=0; j < n; j++)
                {
                    unsigned char o = (unsigned char)(*inCur++);
                    if ((o & 0xc0) != 0x80)
                    {
                        throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
                    }
                    ch = (ch << 6) | (o & 0x3f);
                }

                if (n > 1 && !(ch >> (n * 5 + 1)))
                    throw ScopeStreamException(ScopeStreamException::InvalidCharacter);

                u8len -= n;

                // Save the last read input position, because we have consumed one unicode character.
                inMid = inCur;

                if (ch < 0x80 && x_UTF7DirectChars[ch])
                {
                    if (m_bitCount >= 0)
                    {
                        if (m_bitCount > 0)
                        {
                            // Try to add the next byte
                            if (outMid < outLast)
                                *outMid++ = x_Base64Chars[(m_bits << (6 - m_bitCount)) & 0x3F];
                            else
                                return;

                            m_bitCount = 0;
                        }

                        // Need to get emit '-' and our char, 2 bytes total
                        if (outMid < outLast)
                        {
                            *outMid++ = '-';
                        }
                        else
                            return;

                        m_bitCount = -1;
                    }

                    // Need to emit our char
                    if (outMid < outLast)
                        *outMid++ = (char)ch;
                    else
                        return;
                }
                else if (m_bitCount < 0 && ch == (unsigned short)'+')
                {
                    if (outMid + 1 < outLast)
                    {
                        *outMid++ = '+';
                        *outMid++ = '-';
                    }
                    else
                        return;
                }
                else
                {
                    if (m_bitCount < 0)
                    {
                        // Need to emit a + and 12 bits (3 bytes)
                        // Only 12 of the 16 bits will be emitted this time, the other 4 wait 'til next time
                        if (outMid < outLast)
                        {
                            *outMid++ = '+';
                        }
                        else
                            return;

                        // We're now in bit mode, but haven't stored data yet
                        m_bitCount = 0;
                    }

                    m_bits = m_bits << 16 | ch;
                    m_bitCount += 16;

                    while (m_bitCount >= 6)
                    {
                        // Try to add the next byte
                        if (outMid < outLast)
                            *outMid++ = x_Base64Chars[(m_bits >> (m_bitCount-6)) & 0x3F];
                        else
                            return;

                        m_bitCount -= 6;
                    }

                    SCOPE_ASSERT(m_bitCount < 6);
                }

                //advance the output buffer since we have finished one loop. 
                outMid = outMid;
            }

            // Now if we have bits left over we have to encode them.
            if (m_bitCount >= 0)
            {
                if (m_bitCount > 0)
                {
                    // Try to add the next byte
                    if (outMid < outLast)
                        *outMid++ = x_Base64Chars[(m_bits << (6 - m_bitCount)) & 0x3F];
                    else
                        return;

                    m_bitCount = 0;
                }

                // Need to get emit '-'
                if (outMid < outLast)
                {
                    *outMid++ = '-';
                }
                else
                    return;

                m_bitCount = -1;
            }
        }
    };

    // class to read a buffer of text in different encoding. The result will be converted to utf-8 encoding.
    class TextEncodingReader
    {
        CosmosInput * m_inputStream; // input stream 

        unique_ptr<IncrementalAllocator>      m_alloc;       // allocate for translation target buffer

        char        * m_inStart;     // start position of input buffer
        char        * m_outStart;    // start position of output buffer. 
        SIZE_T        m_remain;      // remain elements in the input buffer

        bool          m_firstRead;
        TextEncoding  m_encoding;

        static const int x_codeCvtBufSize = 8*1024*1024;
        static const int x_underflowBufSize = 20;

        char          m_underflowBuf[x_underflowBufSize];
        SIZE_T        m_underflowRemain;

        // different encoding converter for code translation. They are needed since the encoding is runtime decision for extractor.
        TextEncodingConverter<Unicode>              m_unicodeConverter;
        TextEncodingConverter<BigEndianUnicode>     m_beUnicodeConverter;
        TextEncodingConverter<UTF32>                m_utf32Converter;
        TextEncodingConverter<BigEndianUTF32>       m_beUtf32Converter;
        TextEncodingConverter<UTF7>                 m_utf7Converter;

        static TextEncoding DetectEncodingByBOM(unsigned char * buf, SIZE_T size)
        {
            if (HasUTF32LEBOM(buf, size))
            {
                return UTF32;
            }
            else if (HasUTF32BEBOM(buf, size))
            {
                return BigEndianUTF32;
            }
            else if (HasUTF16LEBOM(buf, size))
            {
                return Unicode;
            }
            else if (HasUTF16BEBOM(buf, size))
            {
                return BigEndianUnicode;
            }
            else if (HasUTF8BOM(buf, size))
            {
                return UTF8;
            }
            else if (HasUTF7BOM(buf, size))
            {
                return UTF7;
            }

            return Default;
        }

        static bool HasUTF32LEBOM(unsigned char * buf, SIZE_T size)
        {
            return size >= 4 &&
                buf[0] == 0xFF &&
                buf[1] == 0xFE &&
                buf[2] == 0 &&
                buf[3] == 0;
        }

        static bool HasUTF32BEBOM(unsigned char * buf, SIZE_T size)
        {
            return size >= 4 &&
                buf[0] == 0 &&
                buf[1] == 0 &&
                buf[2] == 0xFE &&
                buf[3] == 0xFF;
        }

        static bool HasUTF16LEBOM(unsigned char * buf, SIZE_T size)
        {
            return size >= 2 && 
                buf[0] == 0xFF &&
                buf[1] == 0xFE;
        }

        static bool HasUTF16BEBOM(unsigned char * buf, SIZE_T size)
        {
            return size >= 2 &&
                buf[0] == 0xFE &&
                buf[1] == 0xFF;
        }

        static bool HasUTF8BOM(unsigned char * buf, SIZE_T size)
        {
            return size >= 3 &&
                buf[0] == 0xEF &&
                buf[1] == 0xBB &&
                buf[2] == 0xBF;
        }

        static bool HasUTF7BOM(unsigned char * buf, SIZE_T size)
        {
            return size >= 4 &&
                buf[0] == 0x2B &&
                buf[1] == 0x2F &&
                buf[2] == 0x76 &&
                (buf[3] == 0x38 || buf[3] == 0x39 || buf[3] == 0x2B || buf[3] == 0x2F);
        }

        template<TextEncoding encoding>
        void ConvertToUtf8(TextEncodingConverter<encoding> * encoder, SIZE_T & numRead)
        {
            char * pInNext = NULL;
            char * pOutNext = NULL;

            encoder->ConvertToUtf8(m_inStart, 
                &m_inStart[m_remain], 
                pInNext, 
                m_outStart, 
                m_outStart+x_codeCvtBufSize, 
                pOutNext);

            // If nothing gets converted, we will need to fetch one more buffer page and convert again
            if (m_inStart == pInNext)
            {
                // nothing gets output as well.
                SCOPE_ASSERT(m_outStart == pOutNext);
                if (m_remain < x_underflowBufSize)
                {
                    memcpy(m_underflowBuf, pInNext, m_remain);
                    m_underflowRemain = m_remain;
                    m_remain = 0;

                    m_inStart = m_inputStream->GetNextPage(m_remain);

                    // if it is end of stream, raise exception since charater is not enough to convert
                    if (m_inStart == NULL || m_remain == 0)
                    {
                        // if there is remain code not converted, invalid character detected. 
                        throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
                    }

                    SCOPE_ASSERT(m_underflowRemain > 0);

                    SIZE_T nCopy = std::min(x_underflowBufSize - m_underflowRemain, m_remain);
                    memcpy(&m_underflowBuf[m_underflowRemain], m_inStart, nCopy);

                    encoder->ConvertToUtf8(m_underflowBuf, 
                        &m_underflowBuf[m_underflowRemain+nCopy], 
                        pInNext, 
                        m_outStart, 
                        m_outStart+x_codeCvtBufSize, 
                        pOutNext);
                    if ( m_outStart == pOutNext)
                    {
                        // Since underflow can only happen at the beginning of a new buffer, this should never happen.
                        throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
                    }

                    // number of character converted must larger than original m_underflowRemain.
                    SCOPE_ASSERT((SIZE_T)(pInNext - m_underflowBuf) > m_underflowRemain);

                    SIZE_T nProceed = pInNext - m_underflowBuf - m_underflowRemain;

                    m_inStart += nProceed; 
                    m_remain -= nProceed;
                    numRead = pOutNext - m_outStart;
                    return;
                }
                else
                {
                    throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
                }
            }

            numRead = pOutNext - m_outStart;
            m_remain -= (pInNext-m_inStart);
            m_inStart = pInNext;
        }

    public:

        // default encoding is passed in. the encoding will only be used if BOM detection is failed.
        TextEncodingReader(TextEncoding encoding): 
            m_inputStream(NULL), 
            m_inStart(NULL),
            m_outStart(NULL),
            m_remain(0),
            m_firstRead(true),
            m_encoding(encoding),
            m_underflowRemain(0)
        {
        }

        void Init(CosmosInput * inputStream)
        {
            SCOPE_ASSERT(m_inputStream == NULL);

            m_inputStream = inputStream;
            // Initialize the output buffer
            m_alloc.reset(new IncrementalAllocator(x_codeCvtBufSize*2, "TextEncodingReader"));
            m_outStart = m_alloc->Allocate(x_codeCvtBufSize);
        }

        void Close()
        {
            m_inputStream = NULL;
            if (m_alloc)
            {
                m_alloc->Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            }
        }

        void Restart()
        {
            m_inputStream->Restart();
            m_inStart = NULL;
            m_remain = 0;
        }

        // Convert the page read from inputStream to UTF-8 and return.
        // Default implementation has no conversion. It just delegate the call to cosmos stream.
        char * GetNextPage(SIZE_T & numRead)
        {
            SCOPE_ASSERT(m_inputStream != NULL);

            if (m_inStart == NULL || m_remain == 0)
            {
                m_inStart = m_inputStream->GetNextPage(m_remain);
            }

            // if it is end of stream, return NULL
            if (m_inStart == NULL || m_remain == 0)
            {
                numRead = 0;
                return NULL;
            }

            if (m_firstRead && m_encoding == Default)
            {
                m_encoding = DetectEncodingByBOM((unsigned char *)m_inStart, m_remain);
            }

            switch(m_encoding)
            {
            case ASCII:
            case Default:
            case UTF8:
                // There is no need for translation, just return the original buffer
                m_outStart = m_inStart;
                numRead = m_remain;
                m_remain = 0;
                break;

            case Unicode:
                ConvertToUtf8<Unicode>(&m_unicodeConverter, numRead);
                break;

            case BigEndianUnicode:
                ConvertToUtf8<BigEndianUnicode>(&m_beUnicodeConverter, numRead);
                break;

            case UTF7:
                ConvertToUtf8<UTF7>(&m_utf7Converter, numRead);
                break;

            case UTF32:
                ConvertToUtf8<UTF32>(&m_utf32Converter, numRead);
                break;

            case BigEndianUTF32:
                ConvertToUtf8<BigEndianUTF32>(&m_beUtf32Converter, numRead);
                break;
            }

            if (m_firstRead)
            {
                // Remove BOM from the output buffer (UTF-8 representstion of U+FEFF occupies 3 bytes)
                // Do it after we perform conversion as UTF7 encoding requires it (for other encodings it doesn't matter)
                if (HasUTF8BOM((unsigned char *)m_outStart, numRead))
                {
                    m_outStart += 3;
                    numRead -= 3;
                }

                m_firstRead = false;
            }

            return m_outStart;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            if (m_alloc)
            {
                m_alloc->WriteRuntimeStats(root);
            }
        }
    };

    //
    // Static class that contains conversion logic from FString to all SCOPE-supported types.
    //
    class TextConversions
    {
        template<typename T>
        static INLINE bool ConvertToInfNaNHelper(const char * buf, int size, T & out)
        {
            static_assert(is_floating_point<T>::value, "This routine is valid only for floating point types");

            int len = size-1;

            if ((len == sizeof(x_NaN)) && (strncmp(buf, x_NaN, sizeof(x_NaN)) == 0))
            {
                out = numeric_limits<T>::quiet_NaN();
                return true;
            }
            else if ((len == sizeof(x_PositiveInf)) && (strncmp(buf, x_PositiveInf, sizeof(x_PositiveInf)) == 0))
            {
                out = numeric_limits<T>::infinity();
                return true;
            }
            else if ((len == sizeof(x_NegativeInf)) && (strncmp(buf, x_NegativeInf, sizeof(x_NegativeInf)) == 0))
            {
                out = -(numeric_limits<T>::infinity());
                return true;
            }

            return false;
        }

        template<typename T>
        static INLINE bool ConvertToInfNaN(const char *, int, T &)
        {
            return false;
        }

        template<>
        static INLINE bool ConvertToInfNaN<double>(const char * buf, int size, double & out)
        {
            return ConvertToInfNaNHelper(buf, size, out);
        }

        template<>
        static INLINE bool ConvertToInfNaN<float>(const char * buf, int size, float & out)
        {
            return ConvertToInfNaNHelper(buf, size, out);
        }

    public:

        template<typename T>
        static INLINE bool TryFStringToT(FStringWithNull & str, T & out)
        {
            if (is_floating_point<T>::value)
            {
                str.TrimWhiteSpace();

                if (ConvertToInfNaN(str.buffer(), str.size(), out))
                {
                    return true;
                }
            }

            return NumericConvert(str.buffer(), str.size(), out) == ConvertSuccess;
        }

        template<>
        static INLINE bool TryFStringToT<FString>(FStringWithNull & str, FString & out)
        {
            // This transfers string buffer ownership to the "out" object
            return out.ConvertFrom(str);
        }

        template<>
        static INLINE bool TryFStringToT<FBinary>(FStringWithNull & str, FBinary & out)
        {
            // This transfers string buffer ownership to the "out" object
            return out.ConvertFrom(str);
        }

        // convert a bool from string
        // it only takes True and False
        template<>
        static INLINE bool TryFStringToT<bool>(FStringWithNull & str, bool & out)
        {
            str.TrimWhiteSpace();
            unsigned int len = str.DataSize();

            if ((len == sizeof(x_True)) && (_strnicmp(str.buffer(), x_True, sizeof(x_True)) == 0))
            {
                out = true;
            }
            else if ((len == sizeof(x_False)) && (_strnicmp(str.buffer(), x_False, sizeof(x_False)) == 0))
            {
                out = false;
            }
            else 
            {
                return false;
            }

            return true;
        }

        // convert a Decimal from string
        template<>
        static INLINE bool TryFStringToT<ScopeDecimal>(FStringWithNull & str, ScopeDecimal & out)
        {
            try
            {
                str.TrimWhiteSpace();

                // Convert string to decimal
                out = ScopeDecimal::Parse(str.buffer(), str.DataSize());
            }
            catch(ScopeDecimalException &)
            {
                return false;
            }

            return true;
        }

        // convert a DateTime from string
        template<>
        static INLINE bool TryFStringToT<ScopeDateTime>(FStringWithNull & str, ScopeDateTime & out)
        {
            // Try and convert tmp to Date time using managed code
            return ScopeDateTime::TryParse(str.buffer(), out);
        }

        template<>
        static INLINE bool TryFStringToT<ScopeGuid>(FStringWithNull & str, ScopeGuid & out)
        {
            str.TrimWhiteSpace();
            return ScopeGuid::TryParse(str.buffer(), str.DataSize(), out);
        }

#if !defined(SCOPE_NO_UDT)
        // deserialize a scope supported UDT type by invoke IScopeSerializableText interface.
        template<int ColumnTypeID>
        static INLINE bool TryFStringToT(FStringWithNull & str, ScopeUDTColumnTypeStatic<ColumnTypeID> & out)
        {
            try
            {
                // convert tmp to UDT using managed code
                FStringToScopeUDTColumnType(str.buffer(), out);
            }
            catch(ScopeStreamException &)
            {
                return false;
            }

            return true;
        }

        // deserialize a scope supported UDT type by invoke IScopeSerializableText interface.
        static INLINE bool TryFStringToT(FStringWithNull & str, ScopeUDTColumnTypeDynamic & out)
        {
            try
            {
                // convert tmp to UDT using managed code
                UDTManager::GetGlobal()->FStringToScopeUDTColumnType(str.buffer(), out);
            }
            catch(ScopeStreamException &)
            {
                return false;
            }

            return true;
        }
#endif // SCOPE_NO_UDT

        template<typename T>
        static INLINE bool TryFStringToT(FStringWithNull & str, NativeNullable<T> & out)
        {
            // Null or Empty string is treated as null for NativeNullable<T> type.
            if (str.IsNullOrEmpty())
            {
                out.SetNull();
                return true;
            }
            else if (TryFStringToT(str, out.get()))
            {
                out.ClearNull();
                return true;
            }

            return false;
        }
    };
    
    //
    // TextInputStream traits for "batch" mode
    //
    template<unsigned long _delimiter, int _delimiterLen, bool _escape, bool _textQualifier, TextEncoding _encoding, bool _silent>
    struct TextInputStreamTraitsConst
    {
        static const unsigned long delimiter = _delimiter;
        static const int delimiterLen = _delimiterLen;
        static const bool escape = _escape;
        static const bool textQualifier = _textQualifier;
        static const TextEncoding encoding = _encoding;
        static const bool silent = _silent;

        TextInputStreamTraitsConst(const InputStreamParameters &)
        {
        }
    };

    // A wrapper to provide text stream interface on top of Cosmos input which provides block reading interface.
    // It hides the page from the reader.
    // This class is used in DefaultTextExtractor.
    template<class TextInputStreamTraits>
    class TextInputStream : public TextInputStreamTraits, public TextConversions
    {
        unsigned char * m_buffer;        // current reading buffer
        unsigned char * m_startPoint;    // current reading start point
        unsigned char * m_startRecord;   // start of the current record
        SIZE_T  m_numRemain;    // number of character remain in the buffer
        __int64 m_recordLineNumber; // Current record number
        int m_fieldNumber;         // Current field in record being processed
        int m_delimiterCount;      // number of delimiter has seen in current line.
        int m_numColumnExpected;   // number of columns expected from the schema.

        UINT64          m_cbRead;        // how many bytes already read
        InputFileInfo   m_input;         // input file info
        bool            m_adjustCursor;  // for split, this flag indicate whether cursor is adjusted to right offset before reading first record

        bool            m_betweenDoubleQuotes; // to indicate current token is inside double quotes.
        
        // Special values are all negative ints to avoid colliding with a byte value
        enum Token
        {
            TOKEN_NONE = -2,
            TOKEN_EOF = -1,
            TOKEN_NEWLINE = '\n',
            TOKEN_LINEFEED = '\r',
            TOKEN_HASH = '#',
            TOKEN_QUOTE = '"',
        };
        Token m_pushBack;            // Last token pushed back (or TOKEN_NONE if none)

        CosmosInput  m_asyncInput;  // blocked cosmos input stream 
        IncrementalAllocator  * m_allocator;      // memory allocator pointer for deserialization
        TextEncodingReader m_codecvt; // encoding converter instance

        string m_delayBadFormatText; // detail of delay bad format exception 
        bool m_delayBadFormatException; // True if we have badformat exception stored.

        enum TokenizerState
        {
            ReadOneToken = 0,
            NewLine = 1,
            EndOfFile = 2
        };

        FORCE_INLINE bool InQuotedString()
        {
            return textQualifier && m_betweenDoubleQuotes;
        }

        FORCE_INLINE void ResetQuotedString()
        {
            m_betweenDoubleQuotes = false;
        }
        
        // Check if the current token is a the pos byte (counting from 0) of the delimiter
        // return true if it is
        //
        bool CheckDelimiter(Token token, int pos)
        {
            // If delimiter is in quoted string, keep the literal as is
            if (InQuotedString())
            {
                return false;
            }
            
            switch (delimiterLen - pos)
            {
            case 4:
                return token == (unsigned char)((delimiter >> 24) & 0xff);
            case 3:
                return token == (unsigned char)((delimiter >> 16) & 0xff);
            case 2:
                return token == (unsigned char)((delimiter >> 8) & 0xff);
            case 1:
                return token == (unsigned char)(delimiter & 0xff);
            default:
                SCOPE_ASSERT(false);
                return false;
            }
        }

        // Refill the input buffer.
        bool Refill()
        {
            SIZE_T nRead=0;
            char * newBuf = m_codecvt.GetNextPage(nRead);
            if(newBuf == NULL || nRead==0)
                return false;

            m_buffer = (unsigned char *)newBuf;
            m_startPoint = m_buffer;
            m_startRecord = m_buffer;
            m_numRemain = nRead;
            return true;
        }

        void StoreBadFormatException()
        {
            // we only store first BadFormatException per row.
            if (!m_delayBadFormatException)
            {
                m_delayBadFormatException = true;
                stringstream out;

                out << "Problem while converting input record at line " << CurrentLineNumber() << ", column " << CurrentField() << "\n";
                out << "from input stream " << StreamName() << "\n\n";
                Dump(out);

                m_delayBadFormatText = out.str();
            }
        }

        // deserialize a new line
        void EndRow(bool readNewLine)
        {
            if (readNewLine)
            {
                FStringWithNull tmp;

                TokenizerState r = ReadString(tmp);
                // we will ignore endoffile(last line does not contains newline) or newline. 
                if (r != EndOfFile && r != NewLine)
                {
                    throw ScopeStreamException(ScopeStreamException::NewLineExpected);
                }
            }

            // If column does not match schema, throw exception NewLine
            if (m_delimiterCount+1 != m_numColumnExpected)
            {
                throw ScopeStreamException(ScopeStreamException::NewLine);
            }

            // If there is BadFormatException delayed, throw them now.
            if (m_delayBadFormatException)
            {
                throw ScopeStreamException(ScopeStreamException::BadFormat);
            }
        }

        // Get next charater from the input stream (or TOKEN_EOF for end of file)
        // If we've pushed back a token - get it back, otherwise get next
        Token GetToken()
        {
            Token token = TOKEN_NONE;

            if (m_pushBack != TOKEN_NONE)
            {
                token = m_pushBack;
                m_pushBack = TOKEN_NONE;
            }
            else
            {
                token = GetNextToken();
            }

            return token;
        }


        // Get next charater from the input stream (or TOKEN_EOF for end of file)
        // - this is an optimization of the above routine where we know we haven't pushed a token since the last call
        // !!! DO NOT REMOVE "FORCE_INLINE" as it will cause performance regression
        FORCE_INLINE Token GetNextToken()
        {
            // If the buffer is empty - get a new one
            if (m_numRemain == 0)
            {
                if (!Refill())
                    return TOKEN_EOF;
            }

            // return the next character
            m_numRemain--;

            // increase read byte count
            m_cbRead++;

            return (Token)*m_startPoint++;
        }

        void PushToken(Token token)
        {
            SCOPE_ASSERT(m_pushBack == TOKEN_NONE);
            m_pushBack = token;
        }

        // Read string with/without escape translation
        // "autoBuffer" can be NULL which means that data should be discarded
        FORCE_INLINE TokenizerState ReadStringInner(AutoExpandBuffer * autoBuffer)
        {
            // If we've pushed back a token - get it back, otherwise get next
            Token token = GetToken();
            bool hasData = false;

            // Keep scanning until EOF
            while (token != TOKEN_EOF)
            {
                if(token == TOKEN_LINEFEED || token == TOKEN_NEWLINE)
                {
                    // If a string is between quotation mark, and a newline (e.g. "somestring\r\n)
                    // we treated it equal to "somestring"\r\n
                    ResetQuotedString();
                    
                    // if we have data - return it and deal with the newline on the next call
                    if (hasData)
                    {
                        if (autoBuffer != nullptr) autoBuffer->Append('\0');
                        PushToken(token);
                        ++m_fieldNumber;
                        return ReadOneToken;
                    }

                    // 3 different forms of newline (from StreamReader.ReadLine .NET documentation)
                    // '\r' '\n'
                    // '\r'
                    // '\n'
                    //
                    if (token == TOKEN_LINEFEED)
                    {
                        token = GetNextToken();
                        if (token != TOKEN_NEWLINE)
                        {
                            PushToken(token);
                        }
                    }
                    // increase line number and return newline
                    m_recordLineNumber++;
                    return NewLine;
                }

                else if (escape && token == TOKEN_HASH && autoBuffer != nullptr)
                {
                    SIZE_T escapeStartPos = autoBuffer->Size();

                    autoBuffer->Append(x_hash);
                    hasData = true;
                    token = GetNextToken();

                    // Look for the end of the hash tag (or the end of the field)
                    while (token != TOKEN_HASH && token != TOKEN_EOF && !CheckDelimiter(token, 0) && token != TOKEN_NEWLINE && token != TOKEN_LINEFEED)
                    {
                        autoBuffer->Append((char)token);
                        // Already set hasData when we saw opening '#'
                        token = GetNextToken();
                    }

                    if (token == TOKEN_HASH)
                    {
                        // Found a potential hash tag - process it
                        autoBuffer->Append((char)token);
                        // Already set hasData  when we saw opening '#'
                        ExpandHash(autoBuffer, escapeStartPos);
                    }
                    else
                    {
                        // Continue processing with token we ended on - aborted #tag is already in the buffer
                        continue;
                    }
                }

                // handle text qualifier
                else if (textQualifier && token == TOKEN_QUOTE && autoBuffer != nullptr)
                {
                    if(!m_betweenDoubleQuotes)
                    {
                        m_betweenDoubleQuotes = true;
                    }
                    else
                    {
                        int count = 0;
                        
                        do
                        {
                            // handle paired-double-quotes inside "..."
                            // "Hello "" World"  -> Hello " World
                            count++;
                            if ((count & 0x1) == 0) autoBuffer->Append((char)token);
                            token = GetNextToken();
                        }
                        while (token == TOKEN_QUOTE);

                        m_betweenDoubleQuotes = ((count & 0x1) == 0 ? true : false);

                        continue;
                    }                    
                }

                // Check for delimiter
                else if (CheckDelimiter(token, 0))
                {
                    // The following code relies on the properties of UTF8
                    // in a multi byte sequence no follow on byte can match a normal single byte charater as the top bits are always set to a special value
                    // - so if we abort part way through reading the delimiter we know we haven't skipped anything else we may have been interested in
                    // - plus we know we  can continue from the middle of the multi-byte character will no ill effects
                    // Special case for delimiter length of 1 - we've already matched, skip the multi-byte logic
                    if (delimiterLen != 1)
                    {
                        int c;
                        for (c = 1; c < delimiterLen; ++c)
                        {
                            if (autoBuffer != nullptr) autoBuffer->Append((char)token);
                            hasData = true;
                            token = GetNextToken();
                            if (!CheckDelimiter(token, c))
                                break;
                        }

                        // Two cases here - we either found the whole delimiter, or we didn't
                        // If we didn't find it the chars we skipped are in the autoBuffer and token contains the character we stopped on
                        if (c != delimiterLen)
                            continue;

                        // We matched the delimiter - remove it from the autobuffer and return the field
                        // autobuffer doesn't contain the last char we matched (it is still in token)
                        if (autoBuffer != nullptr) autoBuffer->RemoveEnd(delimiterLen - 1);
                    }
                    if (autoBuffer != nullptr) autoBuffer->Append('\0');
                    ++m_delimiterCount;
                    ++m_fieldNumber;
                    return ReadOneToken;
                }

                else
                {
                    // Otherwise add to string we are collecting
                    if (autoBuffer != nullptr) autoBuffer->Append((char)token);
                    hasData = true;
                }

                token = GetNextToken();
            }

            if (hasData)
            {
                if (autoBuffer != nullptr) autoBuffer->Append('\0');
                PushToken(TOKEN_EOF);
                ++m_fieldNumber;
                return ReadOneToken;
            }

            ResetQuotedString();
            return EndOfFile;
        }


        // Expand a #hash# sequence replacing the expanded sequence into the autobuffer
        // #NULL# is not expanded but left alone, if a field contains nothing but #NULL# it will be turned into a null string
        // This is called with the potential # sequence at the end of the auto buffer and not null terminated
        void ExpandHash(AutoExpandBuffer * autoBuffer, SIZE_T escapeStartPos)
        {            
            char * escapeStringStart = &((*autoBuffer)[escapeStartPos]);

            // translate escape string
            switch(autoBuffer->Size() - escapeStartPos)
            {
            case 6:
                if (escapeStringStart[0] == x_hash &&
                    escapeStringStart[1] == 'N' &&
                    escapeStringStart[2] == 'U' &&
                    escapeStringStart[3] == 'L' &&
                    escapeStringStart[4] == 'L' &&
                    escapeStringStart[5] == x_hash )
                {
                    // we have a match for #NULL#, leave it for the caller to handle
                }
                else if (escapeStringStart[0] == x_hash &&
                    escapeStringStart[1] == 'H' &&
                    escapeStringStart[2] == 'A' &&
                    escapeStringStart[3] == 'S' &&
                    escapeStringStart[4] == 'H' &&
                    escapeStringStart[5] == x_hash )
                {
                    // we have a match for #HASH#, remove all but the first '#'
                    autoBuffer->RemoveEnd(5);
                }

                break;

            case 5:
                if (escapeStringStart[0] == x_hash &&
                    escapeStringStart[1] == 'T' &&
                    escapeStringStart[2] == 'A' &&
                    escapeStringStart[3] == 'B' &&
                    escapeStringStart[4] == x_hash)
                {
                    // we have a match for #TAB#
                    // (note this is bit misleading - #TAB# is a replacement for the current delimiter, even if it is not a tab)
                    autoBuffer->RemoveEnd(5);

                    // re-encode delimiter back into characters
                    switch (delimiterLen)
                    {
                    default:
                        SCOPE_ASSERT(false);
                    case 4:
                        autoBuffer->Append((char)((delimiter >> 24) & 0xff));
                        // Fall through
                    case 3:
                        autoBuffer->Append((char)((delimiter >> 16) & 0xff));
                        // Fall through
                    case 2:
                        autoBuffer->Append((char)((delimiter >> 8) & 0xff));
                        // Fall through
                    case 1:
                        autoBuffer->Append((char)(delimiter & 0xff));
                    }
                }
                break;
            case 3:
                if (escapeStringStart[0] == x_hash &&
                    escapeStringStart[1] == 'N' &&
                    escapeStringStart[2] == x_hash )
                {
                    // we have a match for #N#
                    escapeStringStart[0] = '\n';
                    autoBuffer->RemoveEnd(2);
                }
                else if (escapeStringStart[0] == x_hash &&
                    escapeStringStart[1] == 'R' &&
                    escapeStringStart[2] == x_hash )
                {
                    // we have a match for #R#

                    escapeStringStart[0] = '\r';
                    autoBuffer->RemoveEnd(2);
                }                
                break;
            }
        }

        // Read string with/without escape translation
        template <typename T>
        FORCE_INLINE TokenizerState ReadString(T & s)
        {
            AutoExpandBuffer autoBuffer(m_allocator);

            TokenizerState r = ReadStringInner(&autoBuffer);

            if (r == ReadOneToken)
            {
                if (escape && autoBuffer.Size() == 6 + 1 &&
                    autoBuffer[0] == x_hash &&
                    autoBuffer[1] == 'N' &&
                    autoBuffer[2] == 'U' &&
                    autoBuffer[3] == 'L' &&
                    autoBuffer[4] == 'L' &&
                    autoBuffer[5] == x_hash )
                {
                    s.SetNull();
                }
                else
                {
                    // take buffer and make FString of it
                    s.MoveFrom(autoBuffer);
                }
            }

            return r;
        }

        // For split, adjust cursor to right start offset        
        void AdjustCursor()
        {
            m_adjustCursor = true;
            
            // If input stream is not cross splits, or it's first split in global stream, do not advance cursor 
            if (!m_input.inputCrossSplit || m_input.splitStartOffset == 0)
            {
                return;
            }

            // otherwise, advance cursor to pass the first delimitor (\r, \n) to skip the first uncompleted record.
            // previous split should be responsible for the record crossing its split end point.
            Token token;
            
            do
            {
                token = GetToken();
            }
            while (token != TOKEN_LINEFEED && token != TOKEN_NEWLINE && token != TOKEN_EOF);

            if (token == TOKEN_LINEFEED)
            {
                token = GetNextToken();
            }
            
            if (token == TOKEN_NEWLINE)
            {
                token = GetNextToken(); 
            }

            // Handle EOF
            if (token == TOKEN_EOF)
            {
                m_startRecord = NULL;
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
            
            PushToken(token);
        }

        // Does cursor pass split end point. Yes - current split read should stop
        INLINE bool PassSplitEndPoint() const
        {
            if (m_input.inputCrossSplit &&
                m_input.splitStartOffset + cbConsumed() > m_input.splitEndOffset)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        // Count for bytes which are really consumed. This is different from m_cbRead (byte count read from underneath stream)
        // because of m_pushback (a.k.a. token buffer).
        INLINE UINT64 cbConsumed() const
        {
            if (m_pushBack == TOKEN_NONE || m_pushBack == TOKEN_EOF)
            {
                return m_cbRead;
            }
            else
            {
                // If m_pushBack != TOKEN_NONE, means there is one token buffered in m_pushBack.
                // Because all token read from underneath stream will increase m_cbRead by 1, in 
                // this case m_cbRead >= 1, no underflow for (m_cbRead - 1)
                return m_cbRead - 1;
            }
        }
        
    public:
        // constructor 
        TextInputStream(const InputFileInfo & input, IncrementalAllocator * allocator, SIZE_T bufSize, int bufCount, const InputStreamParameters & inputStreamPars) :
            TextInputStreamTraits(inputStreamPars),
            m_numRemain(0), 
            m_buffer(NULL), 
            m_asyncInput(input.inputFileName, bufSize, bufCount), 
            m_input(input),
            m_cbRead(0),
            m_adjustCursor(false),
            m_betweenDoubleQuotes(false),
            m_allocator(allocator),
            m_startPoint(NULL),
            m_startRecord(NULL),
            m_recordLineNumber(1),
            m_fieldNumber(0),
            m_delimiterCount(0),
            m_codecvt(encoding),
            m_delayBadFormatException(false),
            m_numColumnExpected(0),
            m_pushBack(TOKEN_NONE)
        {
        }

        // Init 
        void Init()
        {
            m_asyncInput.Init();
            m_codecvt.Init(&m_asyncInput);

            // Check split start/end offset
            if (m_input.inputCrossSplit)
            {
                SCOPE_ASSERT(m_input.splitEndOffset >= m_input.splitStartOffset);
                SCOPE_ASSERT(m_input.inputFileLength >= m_input.splitEndOffset - m_input.splitStartOffset);
            }
        }

        // Rewind the input stream
        void ReWind()
        {
            //Start to read from beginning again.
            m_codecvt.Restart();

            SIZE_T nRead=0;
            char * newBuf = m_codecvt.GetNextPage(nRead);

            m_buffer = (unsigned char *)newBuf;
            m_startPoint = m_buffer;
            m_startRecord = m_buffer;
            m_numRemain = nRead;
            m_recordLineNumber = 1;
            m_fieldNumber = 0;
            m_delimiterCount = 0;
            m_delayBadFormatException = false;
            m_numColumnExpected = 0;
            m_pushBack = TOKEN_NONE;
        }

        // Close stream
        void Close()
        {
            m_codecvt.Close();
            m_asyncInput.Close();

            m_buffer = NULL;
            m_startPoint = NULL;
            m_startRecord = NULL;
            m_numRemain = 0;
            m_pushBack = TOKEN_NONE;
        }

        // Get current line number for error messages
        __int64 CurrentLineNumber()
        {
            return m_recordLineNumber;
        }

        // Get current field number for error messages
        int CurrentField()
        {
            return m_fieldNumber;
        }

        // Get current number of delimiters for error messages
        int CurrentDelimiter()
        {
            return m_delimiterCount;
        }

        // Return the filename associated with this stream
        std::string StreamName() const
        {
            return m_asyncInput.StreamName();
        }

        std::string GetBadFormatExceptionText()
        {
            return m_delayBadFormatText;
        }

        // Dump current state of stream for error messages
        void Dump(std::stringstream &out) const
        {
            // Try and display on a single line enough of the input stream to give context
            // - we'll assume an old fashioned 80 column screen width
            // - show upto 60 characters before the current position and 10 after
            // - this leaves 10 for unprintable expansion
            unsigned char *start = (m_startPoint - 60) < m_startRecord ? m_startRecord : (m_startPoint - 60);
            unsigned char *end = (m_numRemain < 10) ? m_startPoint + m_numRemain : m_startPoint + 10;
            int dataOutput = 0;
            int markerPos = 0;

            for(unsigned char *pos = start; pos < end; ++pos)
            {
                if (pos == m_startPoint)
                {
                    markerPos = dataOutput;
                }

                if (isprint(*pos))
                {
                    out << *pos;
                    ++dataOutput;
                }
                else if (*pos == x_r || *pos == x_n)
                {
                    out << *pos;
                    dataOutput = 0;
                }
                else if (*pos == '\t')
                {
                    out << "\\t";
                    dataOutput += 2;
                }
                else
                {
                    out << ".";
                    ++dataOutput;
                }
            }
            out << "\n";

            // Display a "marker" indicating where parsing stopped
            for (int c = 0; c < (markerPos ? markerPos : dataOutput); ++c)
                out << " ";
            out << "^\n";
        }

        template<typename T>
        void Read(T & s)
        {
            FStringWithNull str;

            TokenizerState r = ReadString(str);
            if (r == EndOfFile || r == NewLine)
            {
                // Raise a different exception so that we can handle last column is empty string case.
                // T can not accept empty string case by default, raise BadFormat error. 
                if ((m_delimiterCount + 1 == m_numColumnExpected) && !silent)
                {
                    // Need to throw instead of delay the exception. EndRow will not be called in this case. 
                    // All column count mismatch case should already trigger exception at this point. 
                    StoreBadFormatException();
                    throw ScopeStreamException(ScopeStreamException::BadFormat);
                }
                else
                {
                    // One column is expected to read, but got EOF. 
                    // Only raise EndOfFile exception when silent flag is true,
                    // because caller of Read method (in code-gened TextExtractPolicy) will stop
                    // extracting row when catching EndOfFile exception, and no re-throw.
                    if (r == EndOfFile && silent)
                    {
                        throw ScopeStreamException(ScopeStreamException::EndOfFile);
                    }
                    else if (r == EndOfFile && !silent && !IsLastSplit())
                    {
                        throw ScopeStreamException(ScopeStreamException::TooLargeRow);                        
                    }
                    else
                    {
                        throw ScopeStreamException(ScopeStreamException::NewLine);
                    }
                }
            }

            if (str.IsNullOrEmpty())
            {
                // By default T can not be null or empty string.
                StoreBadFormatException();
            }
            else
            {
                // convert to corresponding type
                FStringToT(str, s);
            }

            if (m_fieldNumber == m_numColumnExpected)
            {
                EndRow(true);
            }
        }

        // deserialize a string from text
        template<>
        void Read<FString>(FString & str)
        {
            TokenizerState r = ReadString(str);
            if (r == EndOfFile || r == NewLine)
            {
                // Raise a different exception so that we can handle last column is empty string case.
                if (m_delimiterCount + 1 == m_numColumnExpected)
                {
                    // create an empty string
                    str.SetEmpty();

                    EndRow(false);
                    return;
                }
                else
                {
                    // One column is expected to read, but got EOF. 
                    // Only raise EndOfFile exception when silent flag is true,
                    // because caller of Read method (in code-gened TextExtractPolicy) will stop
                    // extracting row when catching EndOfFile exception, and no re-throw.
                    if (r == EndOfFile && silent)
                    {
                        throw ScopeStreamException(ScopeStreamException::EndOfFile);
                    }
                    else if (r == EndOfFile && !silent && !IsLastSplit())
                    {
                        throw ScopeStreamException(ScopeStreamException::TooLargeRow);                        
                    }                    
                    else
                    {
                        throw ScopeStreamException(ScopeStreamException::NewLine);
                    }
                }
            }

            if (m_fieldNumber == m_numColumnExpected)
            {
                EndRow(true);
            }
        }

        // deserialize a binary string from text
        template<>
        void Read<FBinary>(FBinary & bin)
        {
            FStringWithNull str;

            TokenizerState r = ReadString(str);
            if (r == EndOfFile || r == NewLine)
            {
                // Raise a different exception so that we can handle last column is empty binary string case.
                if (m_delimiterCount + 1 == m_numColumnExpected)
                {
                    // create an empty byte array
                    bin.SetEmpty();

                    EndRow(false);
                    return;
                }
                else
                {
                    // One column is expected to read, but got EOF. 
                    // Only raise EndOfFile exception when silent flag is true,
                    // because caller of Read method (in code-gened TextExtractPolicy) will stop
                    // extracting row when catching EndOfFile exception, and no re-throw.
                    if (r == EndOfFile && silent)
                    {
                        throw ScopeStreamException(ScopeStreamException::EndOfFile);
                    }
                    else if (r == EndOfFile && !silent && !IsLastSplit())
                    {
                        throw ScopeStreamException(ScopeStreamException::TooLargeRow);                        
                    }                    
                    else
                    {
                        throw ScopeStreamException(ScopeStreamException::NewLine);
                    }
                }
            }

            // Convert FString to FBinary
            FStringToT(str, bin);

            if (m_fieldNumber == m_numColumnExpected)
            {
                EndRow(true);
            }
        }

        // deserialize a scope supported nullable type
        template<typename T>
        void Read(NativeNullable<T> & s)
        {
            FStringWithNull tmp;

            TokenizerState r = ReadString(tmp);
            if (r == EndOfFile || r == NewLine)
            {
                // Raise a different exception so that we can handle last column is null case
                // Empty string is treated as null for NativeNullable<T> type.
                if (m_delimiterCount + 1 == m_numColumnExpected)
                {
                    s.SetNull();
                    EndRow(false);
                    return;
                }
                else
                {
                    // One column is expected to read, but got EOF. 
                    // Only raise EndOfFile exception when silent flag is true,
                    // because caller of Read method (in code-gened TextExtractPolicy) will stop
                    // extracting row when catching EndOfFile exception, and no re-throw.
                    if (r == EndOfFile && silent)
                    {
                        throw ScopeStreamException(ScopeStreamException::EndOfFile);
                    }
                    else if (r == EndOfFile && !silent && !IsLastSplit())
                    {
                        throw ScopeStreamException(ScopeStreamException::TooLargeRow);                        
                    }                    
                    else
                    {
                        throw ScopeStreamException(ScopeStreamException::NewLine);
                    }
                }
            }

            if (!TryFStringToT(tmp, s))
            {
                if (silent)
                {
                    s.SetNull();
                }
                else
                {
                    StoreBadFormatException();
                }
            }

            if (m_fieldNumber == m_numColumnExpected)
            {
                EndRow(true);
            }
        }

        template<typename T>
        void FStringToT(FStringWithNull & str, T & out)
        {
            if (!TryFStringToT(str, out))
            {
                StoreBadFormatException();
            }
        }
        
        // Skip all consecutive empty lines.
        // This function only called at the beginning of a row deserialization of the DefaultTextExtractor.
        void StartRow(int numColumnExpected)
        {
            // if havenot adjust curor to right offset, just do it once.
            if (!m_adjustCursor)
            {
                AdjustCursor();
            }

            if (PassSplitEndPoint())
            {
                m_startRecord = NULL;
                throw ScopeStreamException(ScopeStreamException::PassSplitEndPoint);
            }
            
            // Reset allocator for each new Row.
            // This is necessary for default text extractor since the deseralize method will keep looping to skip
            // invalid rows.
            m_allocator->Reset();
            m_delayBadFormatException = false;
            m_fieldNumber = 0;
            m_delimiterCount = 0;
            m_numColumnExpected = numColumnExpected;


            // If we've pushed back a token - get it back, otherwise get next
            Token token = GetToken();

            while (token == TOKEN_LINEFEED || token == TOKEN_NEWLINE)
            {
                if (token == TOKEN_LINEFEED)
                {
                    token = GetNextToken();
                    // If we see a linefeed by itself treat it as a newline
                    // otherwise we'll account for it below
                    if (token != TOKEN_NEWLINE)
                        m_recordLineNumber++;
                }
                else
                {
                    //increase line number
                    m_recordLineNumber++;
                    token = GetNextToken();
                }
            }

            // Handle EOF
            if (token == TOKEN_EOF)
            {
                m_startRecord = NULL;
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }

            PushToken(token);
        }

        // Skip next column
        void SkipColumn()
        {
            TokenizerState r = ReadStringInner(nullptr);
            if (r == EndOfFile || r == NewLine)
            {
                // Raise a different exception so that we can handle last column is empty string case.
                if (m_delimiterCount + 1 == m_numColumnExpected)
                {
                    EndRow(false);
                    return;
                }
                else
                {
                    if (r == EndOfFile)
                    {
                        throw ScopeStreamException(ScopeStreamException::EndOfFile);
                    }
                    else
                    {
                        throw ScopeStreamException(ScopeStreamException::NewLine);
                    }
                }
            }

            if (m_fieldNumber == m_numColumnExpected)
            {
                EndRow(true);
            }
        }

        // Skip the current line.
        void SkipLine()
        {
            // If we've pushed back a token - get it back, otherwise get next
            Token token = GetToken();

            // Read until we see end of line or EOF
            while (token != TOKEN_NEWLINE && token != TOKEN_LINEFEED && token != TOKEN_EOF)
            {
                token = GetNextToken();
            }

            // Handle CR LF
            if (token == TOKEN_LINEFEED)
            {
                token = GetNextToken();
                if (token != TOKEN_NEWLINE)
                {
                    PushToken(token);
                }
            }
            else if (token == TOKEN_EOF)
            {
                PushToken(token);
                return;
            }

            //increase line number
            m_recordLineNumber++;
        }

        PartitionMetadata * ReadMetadata()
        {
            return nullptr;
        }

        void DiscardMetadata()
        {
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_codecvt.WriteRuntimeStats(root);
            m_asyncInput.WriteRuntimeStats(root);
        }

        // return Inclusive time of input stream
        LONGLONG GetInclusiveTimeMillisecond()
        {
            return m_asyncInput.GetInclusiveTimeMillisecond();
        }

        CosmosInput& GetInputer()
        {
            return m_asyncInput;
        }

        bool IsLastSplit() const
        {
            if (m_input.inputCrossSplit)
            {
                return (m_input.splitEndOffset - m_input.splitStartOffset) == m_input.inputFileLength;
            }
            else
            {
                return true;
            }
        }
    };

    // Cosmos output class.
    class SCOPE_RUNTIME_API CosmosOutput : public ExecutionStats
    {
        BlockDevice *           m_device;
        unique_ptr<Scanner, ScannerDeleter> m_scanner;
        Scanner::Statistics     m_statistics;
        const BufferDescriptor* m_outbuffer;
        SIZE_T                  m_outbufferSize;

        SIZE_T            m_maxOnDiskRowSize;
        SIZE_T            m_ioBufSize;
        int               m_ioBufCount;

        bool              m_started;
        char *            m_startPoint;   // write start position
        SIZE_T            m_numRemain;    // number of character remain in the buffer
        SIZE_T            m_position;
        bool              m_maintainBoundariesMode;
        SIZE_T            m_committed;    // position of buffer that the data cannot be flush to disk, only used when maintainBoundariesMode is true
        std::string       m_recoverState; // cached state from load checkpoint

    public:
        CosmosOutput() : 
            m_device(nullptr), 
            m_outbuffer(NULL), 
            m_outbufferSize(0),
            m_maxOnDiskRowSize(Configuration::GetGlobal().GetMaxOnDiskRowSize()),
            m_ioBufSize(IOManager::x_defaultOutputBufSize),
            m_ioBufCount(IOManager::x_defaultOutputBufCount),
            m_started(false),
            m_maintainBoundariesMode(false),
            m_committed(0)
        {
        }

        CosmosOutput(const std::string & filename, SIZE_T bufSize, int bufCount, bool maintainBoundaries = false) : 
            m_device(IOManager::GetGlobal()->GetDevice(filename)), 
            m_outbuffer(nullptr), 
            m_outbufferSize(0),
            m_maxOnDiskRowSize(Configuration::GetGlobal().GetMaxOnDiskRowSize()),
            m_ioBufSize(bufSize),
            m_ioBufCount(bufCount),
            m_started(false),
            m_maintainBoundariesMode(maintainBoundaries),
            m_committed(0)
        {
        }

        CosmosOutput(BlockDevice* device, SIZE_T bufSize, int bufCount, bool maintainBoundaries = false) : 
            m_device(device), 
            m_outbuffer(nullptr), 
            m_outbufferSize(0),
            m_maxOnDiskRowSize(Configuration::GetGlobal().GetMaxOnDiskRowSize()),
            m_ioBufSize(bufSize),
            m_ioBufCount(bufCount),
            m_started(false),
            m_maintainBoundariesMode(maintainBoundaries),
            m_committed(0)
        {
        }

        //
        // Init() for default constructor
        //
        void Init(const std::string & filename, SIZE_T bufSize, int bufCount)
        {
            m_device = IOManager::GetGlobal()->GetDevice(filename);
            m_ioBufSize = bufSize;
            m_ioBufCount = bufCount;

            Init();
        }

        //
        // Init() for CosmosOutput(std::string & filename, SIZE_T bufSize, int bufCount)
        //
        void Init()
        {
            AutoExecStats stats(this);

            //setup scanner and start scanning. 
            m_scanner.reset(Scanner::CreateScanner(m_device, MemoryManager::GetGlobal(), Scanner::STYPE_Create, m_maxOnDiskRowSize, m_ioBufSize, m_ioBufCount));
            m_scanner->Open();

            m_startPoint = nullptr;
            m_numRemain = 0;
            m_position = 0;

            if (m_recoverState.size() != 0)
            {
                m_scanner->LoadState(m_recoverState);
                m_recoverState.clear();
            }
        }

        void Close()
        {
            AutoExecStats stats(this);

            if (m_scanner)
            {
                if (m_scanner->IsOpened())
                {
                    if (m_started)
                    {
                        m_started = false;
                        m_scanner->Finish();
                    }

                    m_scanner->Close();
                }

                // Track statistics before scanner is destroyed.
                m_statistics = m_scanner->GetStatistics();
                m_statistics.ConvertToMilliSeconds();

                m_scanner.reset();
            }
        }

        bool IsOpened()
        {
            return m_scanner != nullptr;
        }

        SIZE_T GetCurrentPosition()
        {
            return m_position;
        }

        SIZE_T RemainBufferSize() const
        {
            return m_numRemain;
        }

        // it reads n bytes data before current position
        // NOTE: it'll only read data from current buffer
        int ReadBack(BYTE* pBuffer, int len)
        {
            if (m_startPoint == nullptr || m_outbuffer == nullptr)
            {
                return 0;
            }

            len = std::min(len, (int)(m_startPoint - (char*)m_outbuffer->m_buffer));
            
            memcpy(pBuffer, m_startPoint - len, len);

            return len;
        }

        void Write(const char * src, unsigned int size)
        {
            if (m_numRemain >= size)
            {
                memcpy(m_startPoint, src, size);
                m_numRemain -= size;
                m_startPoint += size;
                m_position += size;
            }
            else
            {
                if (m_maintainBoundariesMode)
                {
                    if (m_committed != 0 || m_outbuffer == NULL)
                    {
                        Flush();
                        Write(src, size);
                    }
                    else 
                    {
                        ExpandPage();
                        Write(src, size);
                    }
                }
                else
                {
                    while (m_numRemain < size)
                    {
                        // This condition seems like it would be always true, but it is possible to hit this
                        // code right after calling WriteChar, which does not Flush, so m_numRemain would be 0.
                        if (m_numRemain != 0)
                        {
                            // Copy the remaining first, then fetch the next batch
                            memcpy(m_startPoint, src, m_numRemain);
                            src += static_cast<unsigned int>(m_numRemain);
                            size -= static_cast<unsigned int>(m_numRemain);
                            m_position += m_numRemain;
                            m_numRemain = 0;
                        }

                        Flush();
                    }

                    Write(src, size);
                }
            }
        }

        void WriteChar(char b)
        {
            if (m_numRemain >= 1)
            {
                m_numRemain--;
                *m_startPoint++ = b;
                m_position++;
            }
            else
            {
                Flush();

                // we must have get more buffer to writter.
                SCOPE_ASSERT(m_numRemain >= 1);
                WriteChar(b);
            }
        }

        void Commit()
        {
            m_committed = m_outbufferSize - m_numRemain;
        }

        // write the content to outputer
        void Flush(bool forcePersist = false)
        {
            FlushPage(true);
            if (forcePersist)
            {
                m_scanner->WaitForAllPendingOperations();
            }
        }

        void Finish()
        {
            Commit();
            FlushPage(false);
        }

        // Get a new empty page buffer for write.
        char * GetNextPageBuffer(SIZE_T & bufSize)
        {
            AutoExecStats stats(this);

            if (!m_started)
            {
                m_scanner->Start();
                m_started = true;
            }

            bool next = m_scanner->GetNext(m_outbuffer, m_outbufferSize);
            if (!next)
            {
                return NULL;
            }

            bufSize = m_outbufferSize;
            m_startPoint = (char *)(m_outbuffer->m_buffer);
            m_numRemain = bufSize;
            return (char *)(m_outbuffer->m_buffer);
        }

        // Write out a page
        void IssueWritePage(SIZE_T size, bool seal = false)
        {
            AutoExecStats stats(this);

            if (!m_started)
            {
                m_scanner->Start();
                m_started = true;
            }

            if (m_outbuffer)
            {
                m_scanner->PutNext(m_outbuffer, size, seal);
                stats.IncreaseRowCount(size);
            }
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            RuntimeStats::WriteIOStats(root, m_statistics);
            auto & node = root.AddElement("IOBuffers");
            node.AddAttribute(RuntimeStats::MaxBufferCount(), m_ioBufCount);
            node.AddAttribute(RuntimeStats::MaxBufferSize(), m_ioBufSize);
            node.AddAttribute(RuntimeStats::MaxBufferMemory(), m_statistics.m_memorySize);
            node.AddAttribute(RuntimeStats::AvgBufferMemory(), m_statistics.m_memorySize);
        }

        SIZE_T ExpandPage()
        {
            AutoExecStats stats(this);
            SIZE_T expandSize = 0;

            if (m_outbufferSize < m_maxOnDiskRowSize)
            {
                expandSize = std::min<SIZE_T>(m_maxOnDiskRowSize - m_outbufferSize, IOManager::x_defaultOutputBufSize);
                m_scanner->Expand(m_outbuffer, expandSize);
                m_outbufferSize += expandSize;
                m_numRemain += expandSize;
            }
            else
            {
                std::stringstream ss;
                ss << "The row has exceeded the maximum allowed size of " << m_maxOnDiskRowSize/(1024*1024) << "MB";
                throw RuntimeException(E_USER_ROW_TOO_BIG, ss.str().c_str());
            }

            return expandSize;
        }

        // Return the filename associated with this stream
        std::string StreamName() const
        {
            return m_scanner->GetStreamName();
        }

        void SaveState(BinaryOutputStream& output);
        void LoadState(BinaryInputStream& input);

    private:
        void FlushPage(bool fGetNextPage)
        {
            SIZE_T previousPos = m_outbufferSize - m_numRemain;

            // if fGetNextPage is false, that means this is the last append, we have to call IssueWritePage 
            // even there is no data in the buffer
            if (m_outbufferSize != 0 && previousPos == 0 && fGetNextPage)
            {
                return;
            }

            if (m_maintainBoundariesMode)
            {
                IssueWritePage(m_committed, !fGetNextPage /*seal if the last page*/);
                if (!fGetNextPage && previousPos != m_committed)
                {
                    printf("Error: last flush %d, %d, %d", previousPos, m_committed, m_outbufferSize);
                    SCOPE_ASSERT(false);
                }
            }
            else
            {
                IssueWritePage(previousPos, !fGetNextPage /*seal if the last page*/);
            }

            if (fGetNextPage)
            {
                SIZE_T bufSize;
                char* previousBuffer = m_outbuffer == NULL ? NULL : (char*)m_outbuffer->m_buffer;

                GetNextPageBuffer(bufSize);
                if (m_maintainBoundariesMode && m_committed != previousPos)
                {
                    SCOPE_ASSERT(previousBuffer != NULL);

                    SIZE_T count = previousPos - m_committed;
                    // TODO: not perfect refactoring. scanner has to have at least two buffer and the 
                    // previous buffer is not used again until the current is full.
                    memcpy(m_startPoint, previousBuffer + m_committed, count);
                    m_startPoint += count;
                    m_numRemain -= count;
                }

                m_committed = 0;
            }
            else
            {
                m_outbufferSize = 0;
                m_outbuffer = NULL;
                m_startPoint = NULL;
                m_numRemain = 0;
                m_committed = 0;
            }
        }
    };

#pragma warning(disable: 4251)
    // wrap around MemoryOutputInternal
    // So that it can be used by BinaryOutputBase
    class SCOPE_RUNTIME_API MemoryOutput
    {
        // Disallow Copy and Assign
        MemoryOutput(const MemoryOutput&);
        MemoryOutput& operator=(const MemoryOutput&);

        std::shared_ptr<AutoBuffer> m_inner;
        SIZE_T m_pos;

    public:
        MemoryOutput(const std::string filename, SIZE_T arg1, int arg2, bool arg3)
        {
            UNREFERENCED_PARAMETER(filename);
            UNREFERENCED_PARAMETER(arg1);
            UNREFERENCED_PARAMETER(arg2);
            UNREFERENCED_PARAMETER(arg3);

            m_inner.reset(new AutoBuffer(MemoryManager::GetGlobal()));
            m_pos = 0;
        }

        MemoryOutput(MemoryManager* memMgr)
            : m_inner(new AutoBuffer(memMgr)), m_pos(0)
        {  }

        MemoryOutput(std::shared_ptr<AutoBuffer>& inner)
            : m_inner(inner), m_pos(0)
        {  }

        // it reads n bytes data before current position
        // NOTE: it'll only read data from current buffer
        int ReadBack(BYTE* pBuffer, int len)
        {
            len = std::min((int)m_pos, len);
            memcpy(pBuffer, m_inner->Buffer() + (m_pos - len), len);
            return len;
        }

        void Write(const char * src, unsigned int size)
        {
            while (m_inner->Capacity() - m_pos < size)
            {
                // Copy the remaining first, then fetch the next batch
                SIZE_T copied = m_inner->Capacity() - m_pos;
                memcpy(m_inner->Buffer() + m_pos, src, copied);
                src += static_cast<unsigned int>(copied);
                size -= static_cast<unsigned int>(copied);
                m_pos += copied;
                m_inner->Expand();
            }

            memcpy(m_inner->Buffer() + m_pos, src, size);
            m_pos += size;
        }

        void WriteChar(char & b)
        {
            if (m_pos < m_inner->Capacity())
            {
                *(m_inner->Buffer() + m_pos) = b;
            }
            else
            {
                m_inner->Expand();
                WriteChar(b);
            }
            m_pos++;
        }

        void Commit()
        {
        }

        void Init()
        {
        }

        void Close()
        {
        }

        bool IsOpened()
        {
            return true;
        }

        void Flush(bool forcePersist = false)
        {
            UNREFERENCED_PARAMETER(forcePersist);
        }

        void Finish()
        {
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            if (m_inner)
            {
                m_inner->WriteRuntimeStats(root);
            }
        }

        LONGLONG GetInclusiveTimeMillisecond()
        {
            return 0;
        }

        BYTE* Buffer() const
        {
            return m_inner->Buffer();
        }

        SIZE_T Tellp() const
        {
            return m_pos;
        }

        void SaveState(BinaryOutputStream&)
        {
        }

        void LoadState(BinaryInputStream&)
        {
        }
    };
#pragma warning(default: 4251)

    // Output stream class to hide the block interface from the cosmos stream. 
    template<typename OutputType>
    class SCOPE_RUNTIME_API BinaryOutputStreamBase : public StreamWithManagedWrapper
    {
        OutputType    m_asyncOutput;

    public:

        BinaryOutputStreamBase(const std::string& filename, SIZE_T bufSize, int bufCount, bool maintainBoundaries = false) :
            m_asyncOutput(filename, bufSize, bufCount, maintainBoundaries)
        {
        }

        // write the content to outputer
        void Flush(bool forcePersist = false)
        {
            m_asyncOutput.Flush(forcePersist);
        }

        void Finish()
        {
            m_asyncOutput.Finish();
        }

        //
        // Init() for BinaryOutputStream(std::string filename, SIZE_T bufSize, int bufCount)
        //
        void Init()
        {
            m_asyncOutput.Init();
        }

        void Close()
        {
            m_asyncOutput.Close();
        }

        void Commit()
        {
            m_asyncOutput.Commit();
        }

        void Write(const char * src, unsigned int size)
        {
            m_asyncOutput.Write(src, size);
        }

        void WriteChar(char & b)
        {
            m_asyncOutput.WriteChar(b);
        }

        template<typename T>
        void Write(const T& s)
        {
            Write((const char*) &s, sizeof(T));
        }

        template<typename T>
        void WriteFixedArray(const FixedArrayType<T> & fixedArray)
        {
            unsigned int size = fixedArray.size();
            char b;

            do
            {
                b = size & 0x7f;
                size = size >> 7;
                if (size > 0)
                    b |= 0x80;
                WriteChar(b);
            } while (size);

            Write((const char*) fixedArray.buffer(), fixedArray.size());
        }

        void Write(const FString & str)
        {
            WriteFixedArray(str);
        }

        void Write(const FBinary & bin)
        {
            WriteFixedArray(bin);
        }

        template<typename K, typename V>
        void Write(const ScopeMapNative<K, V>& m)
        {
            m.Serialize(this);
        }

        template<typename T>
        void Write(const ScopeArrayNative<T>& s)
        {
            s.Serialize(this);
        }

        void Write(const ScopeDateTime & s)
        {
            __int64 binaryTime = s.ToBinary();
            Write(binaryTime);
        }

        void Write(const ScopeDecimal & s)
        {
            Write(s.Lo32Bit());
            Write(s.Mid32Bit());
            Write(s.Hi32Bit());
            Write(s.SignScale32Bit());
        }

        void Write(const ScopeGuid & s)
        {
            Write((const char *)&s, sizeof(ScopeGuid));
        }

        template<typename T>
        void Write(const NativeNullable<T> & s)
        {
            // we will never call write a null value in binary outputer
            SCOPE_ASSERT(!s.IsNull());

            Write(s.get());
        }

        void Write(const char& c)
        {
            Write<char>(c);
        }

        void Write(const int& c)
        {
            Write<int>(c);
        }

        void WriteMetadata(PartitionMetadata * )
        {
            // do nothing
        }

#if !defined(SCOPE_NO_UDT)
        template<int ColumnTypeID>
        void Write(const ScopeUDTColumnTypeStatic<ColumnTypeID> & s)
        {
            // we will never call write a null value in binary outputer
            SCOPE_ASSERT(!s.IsNull());

            SerializeUDT(s, this);
        }

        void Write(const ScopeUDTColumnTypeDynamic & s)
        {
            // we will never call write a null value in binary outputer
            SCOPE_ASSERT(!s.IsNull());

            SerializeUDT(s, this);
        }
#endif // SCOPE_NO_UDT

        void WriteRuntimeStats(TreeNode & root)
        {
            m_asyncOutput.WriteRuntimeStats(root);
        }

        LONGLONG GetInclusiveTimeMillisecond()
        {
            return m_asyncOutput.GetInclusiveTimeMillisecond();
        }

        OutputType& GetOutputer()
        {
            return m_asyncOutput;
        }
    };

    class SCOPE_RUNTIME_API BinaryOutputStream : public BinaryOutputStreamBase<CosmosOutput>
    {
    public:
        BinaryOutputStream(const std::string & filename, SIZE_T bufSize, int bufCount, bool maintainBoundaries = false) : 
            BinaryOutputStreamBase(filename, bufSize, bufCount, maintainBoundaries)
        { }
    };

    class SCOPE_RUNTIME_API MemoryOutputStream : public BinaryOutputStreamBase<MemoryOutput>
    {
    public:
        MemoryOutputStream() : BinaryOutputStreamBase("-"/*dummy*/, 1/*dummy*/, 1/*dummy*/)
        { }

    };

    //
    // Binary output stream with payload
    //
    template<typename Payload>
    class AugmentedBinaryOutputStream : public BinaryOutputStream
    {
    public:
        AugmentedBinaryOutputStream(const std::string & filename, SIZE_T bufSize, int bufCount, bool maintainBoundaries = false) : BinaryOutputStream(filename, bufSize, bufCount, maintainBoundaries)
        {
        }

        void WriteMetadata(PartitionMetadata * metadata)
        {
            if (!metadata)
            {
                throw MetadataException("Unexpected NULL metadata");
            }

            cout << "Writing metadata" << endl;
            static_cast<Payload*>(metadata)->Serialize(this);
        }
    };

    // A thin wrapper around AutoBuffer that provide scope type awareness 
    // used exclusively by SStream Data serialization.
    class SCOPE_RUNTIME_API SStreamDataOutputStream : public StreamWithManagedWrapper
    {
        // not owning.
        AutoBuffer* m_inner;

    public:
        explicit SStreamDataOutputStream(AutoBuffer* buffer) : m_inner(buffer) {}

        SIZE_T GetPosition() const
        {
            return m_inner->Tellp();
        }

        void WriteChar(char c)
        {
            m_inner->Put((BYTE) c);
        }

        void Write(const char * buf, SIZE_T size) 
        {
            m_inner->Write((BYTE*) buf, size);
        }

        template<typename T>
        void Write(const T& s)
        {
            Write(reinterpret_cast<const char *>(&s), sizeof(T));
        }

        template<>
        void Write(const FString & str)
        {
            Write(str.buffer(), str.size());
        }

        template<>
        void Write(const FBinary & bin)
        {
            Write((const char *) (bin.buffer()), bin.size());
        }

        template<>
        void Write<ScopeDateTime>(const ScopeDateTime & s)
        {
            __int64 binaryTime = s.ToBinary();
            Write(binaryTime);
        }

        template<>
        void Write<ScopeDecimal>(const ScopeDecimal & s)
        {
            Write(s.Lo32Bit());
            Write(s.Mid32Bit());
            Write(s.Hi32Bit());
            Write(s.SignScale32Bit());
        }

        template<>
        void Write<ScopeGuid>(const ScopeGuid & s)
        {
            Write(reinterpret_cast<char *>(const_cast<ScopeGuid *>(&s)), sizeof(ScopeGuid));
        }

        template<typename K, typename V>
        void Write(const ScopeMapNative<K, V> & s)
        {
            s.Serialize(m_inner);
        }

        template<typename T>
        void Write(const ScopeArrayNative<T> & s)
        {
            s.Serialize(m_inner);
        }

        template<typename T>
        void Write(const NativeNullable<T> & s)
        {
            // we will never call write a null value in sstream outputer
            SCOPE_ASSERT(!s.IsNull());

            Write(s.get());
        }

#if !defined(SCOPE_NO_UDT)
        template<int ColumnTypeID>
        void Write(const ScopeUDTColumnTypeStatic<ColumnTypeID> & s)
        {
            // we will never call write a null value in sstream outputer
            SCOPE_ASSERT(!s.IsNull());

            SerializeUDT(s, this);
        }

        template<int ColumnTypeID>
        void SSLibWrite(const ScopeUDTColumnTypeStatic<ColumnTypeID> & s)
        {
            // we will never call write a null value in sstream outputer
            SCOPE_ASSERT(!s.IsNull());

            SSLibSerializeUDT(s, this);
        }
#endif // SCOPE_NO_UDT
    };

    // class to buffer the output text and convert text from utf8 to specified encoding
    template<class TextEncodingWriterTraits>
    class TextEncodingWriter : protected TextEncodingWriterTraits
    {
        CosmosOutput   *  m_asyncOutput;
        char * m_startPoint;   // write start position
        char * m_linePoint;
        SIZE_T m_numRemain;    // number of character remain in the buffer
        SIZE_T m_bufferSize;   // total buffer size
        SIZE_T m_linePos;      // position of the new line withtin the buffer

        bool  m_firstWrite; // we need to write out BOM before the first write
        bool  m_insideLine; // are we in the middle of writing a line

        // write out BOM, by default there is not BOM for UTF8, ASCII, Default
        void WriteBOM()
        {
            switch(encoding)
            {
            case Unicode:
                OutputChar(0xFF);
                OutputChar(0xFE);
                break;

            case BigEndianUnicode:
                OutputChar(0xFE);
                OutputChar(0xFF);
                break;

            case UTF32:
                OutputChar(0xFF);
                OutputChar(0xFE);
                OutputChar(0);
                OutputChar(0);
                break;

            case BigEndianUTF32:
                OutputChar(0);
                OutputChar(0);
                OutputChar(0xFE);
                OutputChar(0xFF);
                break;
            }
        }

        FORCE_INLINE void OutputChar(const unsigned char & b)
        {
            if (m_numRemain >= 1)
            {
                m_numRemain--;
                *m_startPoint++ = (char)b;
            }
            else
            {
                FlushPage(true, true);
                if (m_numRemain >= 1)
                {
                    m_numRemain--;
                    *m_startPoint++ = (char)b;
                }
            }
        }

    public:
        TextEncodingWriter(CosmosOutput * output):
            m_asyncOutput(output), 
            m_firstWrite(true),
            m_startPoint(NULL),
            m_linePoint(NULL),
            m_numRemain(0),
            m_bufferSize(0),
            m_linePos(0),
            m_insideLine(false)
        {
        }

        TextEncodingWriter(CosmosOutput * output, const OutputStreamParameters & outputStreamParams) :
            TextEncodingWriterTraits(outputStreamParams),
            m_asyncOutput(output), 
            m_firstWrite(true),
            m_startPoint(NULL),
            m_linePoint(NULL),
            m_numRemain(0),
            m_bufferSize(0),
            m_linePos(0),
            m_insideLine(false)
        {
        }

        void FlushPage(bool fGetNextPage, bool expandPage)
        {
            // the last flush automatically "starts" a new line
            if (!fGetNextPage)
            {
                StartNewLine();
            }

            if (m_insideLine)
            {
                if (expandPage)
                {
                    SCOPE_ASSERT(fGetNextPage);
                    SIZE_T expandSize = m_asyncOutput->ExpandPage();

                    m_bufferSize += expandSize;
                    m_numRemain += expandSize;
                }
            }
            else
            {
                // if we are not at the end of the processing, and we don't have any data available,
                // we don't have to flush the current buffer. A special case is that for the first write
                // we don't have a buffer at all, and in this case we have to excuse the below code to get the
                // buffer. This is a matching logic in CosmosOutput.IssueWritePage to ignore the first empty write              
                if (fGetNextPage && m_linePos == 0 && m_startPoint != nullptr)
                {
                    return;
                }

                m_asyncOutput->IssueWritePage(m_linePos, !fGetNextPage /*seal if the last page*/);

                SIZE_T reminder = m_startPoint - m_linePoint;

                if (fGetNextPage)
                {
                    m_startPoint = m_asyncOutput->GetNextPageBuffer(m_bufferSize);
                    m_numRemain = m_bufferSize;

                    memcpy(m_startPoint, m_linePoint, reminder);
                    m_startPoint += reminder;
                    m_numRemain -= reminder;

                    m_linePos = 0;
                    m_insideLine = true;
                }
                else
                {
                    SCOPE_ASSERT(reminder == 0);
                    m_startPoint = NULL;
                    m_numRemain = 0;
                }
            }
        }

        void StartNewLine()
        {
            m_linePos = m_bufferSize - m_numRemain;
            m_linePoint = m_startPoint;
            m_insideLine = false;
        }

        // write string to output stream. It will handle output buffer full. 
        FORCE_INLINE void WriteToOutput(const char * src, SIZE_T size)
        {
            for(;;)
            {
                if (m_numRemain == 0)
                {
                    FlushPage(true, true);
                }

                if (m_firstWrite)
                {
                    WriteBOM();
                    m_firstWrite = false;
                }

                char * pInNext = const_cast<char *>(src);
                char * pOutNext = m_startPoint;

                ConvertFromUtf8(const_cast<char *>(src),
                    const_cast<char *>(src)+size,
                    pInNext,
                    m_startPoint,
                    m_startPoint+m_numRemain,
                    pOutNext);

                SIZE_T nConverted = pInNext - src;

                // If not all input converted
                if (nConverted < size)
                {
                    src = pInNext;
                    size -= nConverted;

                    // Write out page 
                    m_numRemain -= pOutNext - m_startPoint; 
                    m_startPoint = pOutNext;
                    FlushPage(true, true);
                }
                else
                {
                    m_numRemain -= pOutNext - m_startPoint;
                    m_startPoint = pOutNext;
                    return;
                }
            }
        }

        void WriteChar(const unsigned char & b)
        {
            WriteToOutput((char*)&b, sizeof(char));
        }
    };

    //
    // TextOutputStream traits for "batch" mode
    //
    template<unsigned long _delimiter, 
             int _delimiterLen, 
             bool _escape, 
             bool _escapeDelimiter, 
             bool _textQualifier, 
             bool _doubleToFloat, 
             bool _usingDateTimeFormat, 
             TextEncoding _encoding>
    struct TextOutputStreamTraitsConst
    {
        static const unsigned long delimiter = _delimiter;
        static const int delimiterLen = _delimiterLen;
        static const bool escape = _escape;
        static const bool escapeDelimiter = _escapeDelimiter;
        static const bool textQualifier = _textQualifier;
        static const bool doubleToFloat = _doubleToFloat;
        static const bool usingDateTimeFormat = _usingDateTimeFormat;
        static const TextEncoding encoding = _encoding;

        const char * dateTimeFormat;

        TextOutputStreamTraitsConst(const OutputStreamParameters & pars) :
            dateTimeFormat(pars.dateTimeFormat)
        {
        }

        void ConvertFromUtf8(char * inFirst, char * inLast, char *& inMid, char * outFirst, char * outLast, char *& outMid)
        {
            m_codecConverter.ConvertFromUtf8(inFirst, inLast, inMid, outFirst, outLast, outMid);
        }

    private:
        TextEncodingConverter<_encoding> m_codecConverter;
    };

    // Base class for all text output streams
    // Needed for SerializeUDT function
    class TextOutputStreamBase : public StreamWithManagedWrapper
    {
        typedef void (*WriteBufferStubType)(void *, const char *, SIZE_T);

        void * m_objectPtr; // object pointer
        WriteBufferStubType m_writePtr; // "write" method pointer

        template <class T, void (T::*TMethod)(const char *, SIZE_T)>
        static void WriteBufferStub(void* objectPtr, const char * buffer, SIZE_T count)
        {
            T* p = static_cast<T*>(objectPtr);
            return (p->*TMethod)(buffer, count);
        }

    public:
        template <class OutputStream>
        TextOutputStreamBase(OutputStream * objectPtr)
        {
            m_objectPtr = objectPtr;
            m_writePtr  = &WriteBufferStub<OutputStream, reinterpret_cast<void (OutputStream::*)(const char *, SIZE_T)>(&OutputStream::WriteToOutput)>;
        }

        void WriteToOutput(const char * buffer, SIZE_T count)
        {
            (*m_writePtr)(m_objectPtr, buffer, count);
        }
    };

    // Text Output stream class to hide the block interface from the cosmos stream. 
    // It hides the page from the writer. This class is used in defaulttextoutputer.
    template<class TextOutputStreamTraits>
    class TextOutputStream : protected TextOutputStreamTraits, public TextOutputStreamBase
    {
        CosmosOutput    m_asyncOutput;

        TextEncodingWriter<TextOutputStreamTraits> m_codecvt; // code convert for write

        const static __int64 x_epochTicks = 621355968000000000;
        const static __int64 x_unixTimeUpBoundTicks = 946708128000000000;

        bool CheckDelimiter(const char *ptr)
        {
            switch (delimiterLen)
            {
            case 4:
                if (ptr[delimiterLen - 4] != (char)((delimiter >> 24) & 0xff))
                    return false;
                // Fall through
            case 3:
                if (ptr[delimiterLen - 3] != (char)((delimiter >> 16) & 0xff))
                    return false;
                // Fall through
            case 2:
                if (ptr[delimiterLen - 2] != (char)((delimiter >> 8) & 0xff))
                    return false;
                // Fall through
            case 1:
                return ptr[delimiterLen - 1] == (char)(delimiter & 0xff);
            default:
                SCOPE_ASSERT(false);
                return false;
            }
        }

        template<typename T>
        FORCE_INLINE void WriteInteger(T val, bool trimZero)
        {
            char buf[numeric_limits<T>::digits10+2];
            char *it = &buf[numeric_limits<T>::digits10];

            if(!(numeric_limits<T>::is_signed) || val>=0)
            {
                T div = val/100;

                while(div) 
                {
                    memcpy(it, &x_digit_pairs[2*(val-div*100)], 2);
                    val = div;
                    it-=2;
                    div = val/100;
                }

                memcpy(it,&x_digit_pairs[2*val],2);

                // if last pair is less single digital, adjust the start the point.
                if(trimZero && val<10)
                {
                    it++;
                }
            }
            else
            {
                T div = val/100;

                while(div)
                {
                    memcpy(it, &x_digit_pairs[-2*(val-div*100)], 2);
                    val = div;
                    it-=2;
                    div = val/100;
                }

                memcpy(it, &x_digit_pairs[-2*val], 2);

                // if last pair is less single digital, adjust the start the point.
                if(!trimZero || val <= -10)
                {
                    it--;
                }

                *it = '-';
            }

            WriteToOutput(it, (SIZE_T)(&buf[numeric_limits<T>::digits10+2]-it));
        }

        void FlushPage(bool fGetNextPage)
        {
            m_codecvt.FlushPage(fGetNextPage, false);
        }

        //Handling special NaN and Infinity value for double/float type
        template<typename T>
        FORCE_INLINE bool WriteInfNaN(const T & s)
        {
            if (s != s)
            {
                WriteToOutput(x_NaN, sizeof(x_NaN));
                return true;
            }
            else if (s == numeric_limits<T>::infinity())
            {
                WriteToOutput(x_PositiveInf, sizeof(x_PositiveInf));
                return true;
            }
            else if (-s == numeric_limits<T>::infinity())
            {
                WriteToOutput(x_NegativeInf, sizeof(x_NegativeInf));
                return true;
            }

            return false;
        }

    public:
        TextOutputStream(std::string filename, SIZE_T bufSize, int bufCount, bool maintainBoundary = false) :
            TextOutputStreamBase(this),
            m_asyncOutput(filename, bufSize, bufCount, maintainBoundary),  
            m_codecvt(&m_asyncOutput)
        {
        }

        TextOutputStream(std::string filename, SIZE_T bufSize, int bufCount, const OutputStreamParameters & outputStreamParams, bool maintainBoundary = false) :
            TextOutputStreamTraits(outputStreamParams),
            TextOutputStreamBase(this),
            m_asyncOutput(filename, bufSize, bufCount, maintainBoundary),
            m_codecvt(&m_asyncOutput, outputStreamParams)
        {
        }

        //
        // Init() for default constructor
        //
        void Init()
        {
            m_asyncOutput.Init();
        }

        void Close()
        {
            m_asyncOutput.Close();
        }

        // TODO: (weilin) we need refactor the code to remove the maintainboundaries logic in this class 
        // since we already did that in cosmosoutput
        void Commit()
        {
        }

        // write string to output stream. It will handle output buffer full. 
        void WriteToOutput(const char * src, SIZE_T size)
        {
            m_codecvt.WriteToOutput(src, size);
        }

        void StartNewLine()
        {
            m_codecvt.StartNewLine();
        }

        FORCE_INLINE void WriteString(const char * src, SIZE_T size, bool quoted)
        {
            // left quotation mark
            if (quoted)
            {
                WriteToOutput(&x_quote, sizeof(x_quote));
            }
            
            SIZE_T start = 0;
            SIZE_T i = 0;

            for ( i = 0; i < size; i++)
            {
                // \r is escaped when escapeDelimiter flag is true
                if (src[i] == x_r && escapeDelimiter)
                {
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i-start);
                    }

                    start = i+1;
                    WriteToOutput(x_escR, sizeof(x_escR));
                }
                // \n is escaped when escapeDelimiter flag is true             
                else if (src[i] == x_n && escapeDelimiter)
                {
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i-start);
                    }

                    start = i+1;
                    WriteToOutput(x_escN, sizeof(x_escN));
                }
                // Quotation mark inside "..." becomes ""
                else if (quoted && src[i] == x_quote)                
                {
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i-start);
                    }

                    start = i+1;
                    WriteToOutput(x_pairquote, sizeof(x_pairquote));
                }
                else if (src[i] == x_hash && escape)
                {
                    // We only escape hash when escape flag is turned on.
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i-start);
                    }

                    start = i+1;
                    WriteToOutput(x_escHash, sizeof(x_escHash));
                }
                // column delimiter is escaped if not inside "...", and, escapeDelimiter flag is true
                else if (!quoted && escapeDelimiter && CheckDelimiter(src + i))
                {
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i-start);
                    }

                    start = i + delimiterLen;
                    WriteToOutput(x_tab, sizeof(x_tab));
                }
            }

            // write out remaining
            if (i > start && start < size)
            {
                WriteToOutput(&src[start], i-start);
            }

            // right quotation mark
            if (quoted)
            {
                WriteToOutput(&x_quote, sizeof(x_quote));
            }
        }

        void WriteChar(const char & b)
        {
            m_codecvt.WriteChar(b);
        }

        void Flush(bool forcePersist = false)
        {
            FlushPage(true);
            if (forcePersist)
            {
                m_asyncOutput.Flush(true);
            }
        }

        void Finish()
        {
            FlushPage(false);
        }

        template<typename T>
        void Write(const T& s);

        template<>
        void Write<char>(const char &s)
        {
            WriteInteger(s, true);
        }

        template<>
        void Write<unsigned char>(const unsigned char &s)
        {
            WriteInteger(s, true);
        }

        template<>
        void Write<short>(const short &s)
        {
            WriteInteger(s, true);
        }

        template<>
        void Write<unsigned short>(const unsigned short &s)
        {
            WriteInteger(s, true);
        }

        template<>
        void Write<wchar_t>(const wchar_t &s)
        {
            WriteInteger(s, true);
        }

        template<>
        void Write<int>(const int &s)
        {
            WriteInteger(s, true);
        }

        template<>
        void Write<unsigned int>(const unsigned int &s)
        {
            WriteInteger(s, true);
        }

        template<>
        void Write<__int64>(const __int64 &s)
        {
            WriteInteger(s, true);
        }

        template<>
        void Write<unsigned __int64>(const unsigned __int64 &s)
        {
            WriteInteger(s, true);
        }

        template<>
        void Write<bool>(const bool & s)
        {
            if (s)
            {
                WriteToOutput(x_True, sizeof(x_True));
            }
            else
            {
                WriteToOutput(x_False, sizeof(x_False));
            }
        }

        template<>
        void Write<FString>(const FString & s)
        {
            if (s.IsNull())
            {
                // handle null value
                if (escape)
                {
                    WriteNull();
                }

                return;
            }

            WriteString(s.buffer(), s.size(), textQualifier);
        }

        template<>
        void Write<FBinary>(const FBinary & s)
        {
            if (s.IsNull())
            {
                // handle null value
                if (escape)
                {
                    WriteNull();
                }

                return;
            }

            // If it is an empty binary, nothing left to write
            if (s.IsEmpty())
            {
                return;
            }

            // convert to "X2" format string
            auto_ptr<char> buf(new char[s.size()*2]);

            unsigned int j = 0;
            const unsigned char * data = s.buffer();

            for(unsigned int i=0; i < s.size(); i++)
            {
                unsigned char t = data[i];
                unsigned char hi = (t>>4)&0xF;
                unsigned char low = t&0xF;

                buf.get()[j++] = x_HexTable[hi];
                buf.get()[j++] = x_HexTable[low];
            }

            WriteString(buf.get(), j, false);
        }

        FORCE_INLINE void WriteDouble(const double & s)
        {
            // Enable two digit for exponent
            _set_output_format(_TWO_DIGIT_EXPONENT);

            // Check if it is special float/double value that needs special handling.
            if (WriteInfNaN(s))
                return;

            char buf[100];

            int n = sprintf_s(buf, "%.15G", s);

            // read the value back for round-trip testing.
            double tmp = strtod(buf, NULL);

            // If 15 digit precision is not enough for the round-trip, we need
            // to use 17 digit precision.
            if (tmp != s)
            {
                n = sprintf_s(buf, "%.17G", s);
            }

            SCOPE_ASSERT(n>0);

            if ( n > 0 )
            {
                WriteString(buf, n, false);
            }
        }

        template<>
        void Write<double>(const double & s)
        {
            if (doubleToFloat)
            {
                float f = (float)s;
                Write(f);
            }
            else
            {
                WriteDouble(s);
            }
        }

        template<>
        void Write<float>(const float & s)
        {
            // Enable two digit for exponent
            _set_output_format(_TWO_DIGIT_EXPONENT);

            // Check if it is special float/double value that needs special handling.
            if (WriteInfNaN(s))
                return;

            char buf[100];

            int n = sprintf_s(buf, "%.7G", s);

            // read the value back for round-trip testing.
            double tmp = strtod(buf, NULL);
            if (tmp < -FLT_MAX || tmp > FLT_MAX)
            {
                // if value is overflow use 9 digit precision
                n = sprintf_s(buf, "%.9G", s);
            }
            else
            {
                // If 7 digit precision is not enough for the round-trip, we need
                // to use 9 digit precision.
                if (s != (float)tmp)
                {
                    n = sprintf_s(buf, "%.9G", s);
                }
            }

            SCOPE_ASSERT(n>0);

            if ( n > 0 )
            {
                WriteString(buf, n, false);
            }
        }

        template<typename T>
        void Write(const NativeNullable<T> & s)
        {
            if(s.IsNull())
            {
                // handle null value
                if (escape)
                {
                    WriteNull();
                }

                return;
            }

            Write(s.get());
        }

        template<>
        void Write<ScopeDateTime>(const ScopeDateTime & s)
        {
            char buffer [80];
            int n = 0;

            n = s.ToString(buffer, 80, dateTimeFormat);
            if ( n > 0 )
            {
                WriteString(buffer, n, false);
            }
        }

        template<>
        void Write<ScopeDecimal>(const ScopeDecimal & s)
        {
            char finalOut[80];
            int n = ScopeDecimalToString(s, finalOut, 80);

            if (n > 0)
            {
                WriteString(finalOut, n, false);
            }
        }

        template<>
        void Write<ScopeGuid>(const ScopeGuid & s)
        {
            string str = ScopeEngine::GuidToString(s.get());

            if (!str.empty())
            {
                WriteString(str.c_str(), str.length(), false);
            }
        }

#if !defined(SCOPE_NO_UDT)
        // Serialize a scope supported UDT type
        template<int ColumnTypeID>
        void Write(const ScopeUDTColumnTypeStatic<ColumnTypeID> & s)
        {
            if(s.IsNull())
            {
                // handle null value
                if (escape)
                {
                    WriteNull();
                }

                return;
            }

            SerializeUDT(s, this);
        }

        // Serialize a scope supported UDT type
        void Write(const ScopeUDTColumnTypeDynamic & s)
        {
            if(s.IsNull())
            {
                // handle null value
                if (escape)
                {
                    WriteNull();
                }

                return;
            }

            UDTManager::GetGlobal()->SerializeUDT(s, this);
        }
#endif // SCOPE_NO_UDT

        void WriteNewLine()
        {
            // we don't need to escape the new line
            WriteToOutput(x_newline, sizeof(x_newline));
            // signal the new line - possible start of a new extent
            StartNewLine();
        }

        void WriteDelimiter()
        {
            // we don't need to escape the delimiter
            // re-encode delimiter back into characters
            switch (delimiterLen)
            {
            default:
                SCOPE_ASSERT(false);
            case 4:
                WriteChar((delimiter >> 24) & 0xff);
                // Fall through
            case 3:
                WriteChar((delimiter >> 16) & 0xff);
                // Fall through
            case 2:
                WriteChar((delimiter >> 8) & 0xff);
                // Fall through
            case 1:
                WriteChar(delimiter & 0xff);
            }
        }

        void WriteNull()
        {
            // we don't need to escape the null
            WriteToOutput(x_null, sizeof(x_null));
        }

        void WriteMetadata(PartitionMetadata *)
        {
            // do nothing
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_asyncOutput.WriteRuntimeStats(root);
        }

        LONGLONG GetInclusiveTimeMillisecond()
        {
            return m_asyncOutput.GetInclusiveTimeMillisecond();
        }

        CosmosOutput& GetOutputer()
        {
            return m_asyncOutput;
        }
    };

    //
    // SchemaDef serialization
    //

    template<class InputStream>
    class StreamReader
    {
    public:
        template<class T>
        static void Do(InputStream & stream, T & t)
        {
            stream.Read(t);
        }
    };

    template<class OutputStream>
    class StreamWriter
    {
    public:
        template<class T>
        static void Do(OutputStream & stream, const T & t)
        {
            stream.Write(t);
        }
    };

    template<> 
    class BinaryOutputPolicy<SchemaDef>
    {
    public:
        static void Serialize(BinaryOutputStream * output, SchemaDef & row)
        {
            UNREFERENCED_PARAMETER(output);
            UNREFERENCED_PARAMETER(row);
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Not implement for BinaryOutputPolicy::Serialize(MemoryOutputStream, SchemaDef&)");
        }
    };

    class NonExistentPartitionMetadata : public PartitionMetadata
    {
        __int64 m_partitionIndex;
    public:    
        static PartitionMetadata* CreateNonExistentPartitionMetadata()
        {
            return new NonExistentPartitionMetadata(PartitionMetadata::PARTITION_NOT_EXIST);
        }

        NonExistentPartitionMetadata(__int64 partitionIndex) : m_partitionIndex(partitionIndex)
        {
        }

        __int64 GetPartitionId() const
        {
            return m_partitionIndex;
        }

        int GetMetadataId() const
        {
            return -1;
        }

        void Serialize(BinaryOutputStream * stream)
        {
            stream->Write(m_partitionIndex);
            unsigned char flags = 0;
            stream->Write(flags);
        }

        void WriteRuntimeStats(TreeNode & )
        {
        }
    };

    struct StreamingInputParams
    {
        volatile UINT64     Sn;
        bool                NeedSkipInputRecovery;

        StreamingInputParams()
        {
            Sn = 0;
            NeedSkipInputRecovery = false;
        }
    };

#pragma pack(push)
#pragma pack(1)
    class ScopeCEPCheckpointManager
    {
    private:
        IncrementalAllocator* m_alloc;
        CRITICAL_SECTION m_criticalSection;
        UINT64 m_seqNumber;
        ScopeDateTime m_lastCTITime;
        ScopeDateTime m_lastCheckpointWallclockTime;
        ScopeDateTime m_lastCheckpointLogicalTime;
        ScopeDateTime m_startCTITime;
        std::string m_checkpointUriPrefix;
        std::string m_currentScopeCEPState;
        std::string m_lastScopeCEPCheckpointState;
        std::string m_startScopeCEPState;
        UINT m_minimalSecondsCheckpointWallclockInterval;
        UINT m_minimalSecondsCheckpointLogicalTimeInterval;
        UINT m_currentCheckpointId;
        UINT m_inputCount;
        ScopeCEPMode m_scopeCEPMode;
        bool m_startReport;
        StreamingInputParams* m_inputParams;

    public:
        ScopeCEPCheckpointManager() 
            : m_seqNumber(0), 
            m_currentCheckpointId(0), 
            m_inputParams(nullptr),
            m_startReport(false), 
            m_inputCount(0), 
            m_scopeCEPMode(ScopeCEPMode::SCOPECEP_MODE_NONE),
            m_alloc(NULL),
            m_minimalSecondsCheckpointWallclockInterval(300),
            m_minimalSecondsCheckpointLogicalTimeInterval(900),
            m_lastCheckpointWallclockTime(ScopeDateTime::MinValue),
            m_lastCheckpointLogicalTime(ScopeDateTime::MinValue),
            m_startCTITime(ScopeDateTime::MinValue)
        {
            InitializeCriticalSectionAndSpinCount(&m_criticalSection, 10);
        }

        void SetMinimalCheckpointInterval(UINT seconds) { m_minimalSecondsCheckpointWallclockInterval = seconds; }

        void Reset()
        {
            m_seqNumber = 0;
            m_currentCheckpointId = 0;
            if (m_inputParams) 
            {
                delete[] m_inputParams;
                m_inputParams = nullptr;
            }
            m_startReport = false;
            m_inputCount = 0;
            m_scopeCEPMode = SCOPECEP_MODE_NONE;
            m_lastCTITime = ScopeDateTime::MinValue;
            m_startCTITime = ScopeDateTime::MinValue;
        }

        void SetScopeCEPModeAndStartState(ScopeCEPMode mode, const std::string& state, const std::string& checkpointUriPrefix)
        {
            m_scopeCEPMode = mode;
            if (state.compare("null") != 0)
            {
                m_startScopeCEPState = state;
            }
            m_checkpointUriPrefix = checkpointUriPrefix;
        }

        void StartReport()
        {
            m_startReport = true;
        }

        ~ScopeCEPCheckpointManager() 
        { 
            delete [] m_inputParams;
            if (!m_alloc)
            {
                delete m_alloc;
            }
        }

        std::string GenerateCheckpointUri()
        {
            char buf[64];
            memset(buf, 0, 64);
            _itoa_s(m_currentCheckpointId++, buf, 64, 10);
            return m_checkpointUriPrefix + buf;
        }

        UINT GetCurrentCheckpointId() const { return m_currentCheckpointId - 1; } 

        const string GetCurrentScopeCEPState()
        {
            AutoCriticalSection aCS(&m_criticalSection);
            string ret = m_lastScopeCEPCheckpointState;
            ret += "\n";
            ret += m_currentScopeCEPState;
            return ret;
        }

        string& GetStartScopeCEPState() {  return m_startScopeCEPState; }
        ScopeDateTime GetStartCTITime() {  return m_startCTITime; }

        void SetScopeCEPMode(ScopeCEPMode mode) { m_scopeCEPMode = mode; }
        ScopeCEPMode GetScopeCEPMode() { return m_scopeCEPMode; }

        void SetCheckpointUriPrefix(const std::string& prefix) { m_checkpointUriPrefix = prefix; }
        void UpdateCurrentScopeCEPState(const std::string * uri = nullptr)
        {
            AutoCriticalSection aCS(&m_criticalSection);

            if (m_startReport)
            {
                ScopeDateTime now = ScopeDateTime::Now();
                ostringstream ss;
                ss << m_lastCTITime.ToFileTime(true) << ";";
                ss << m_seqNumber << ";" << m_inputCount << ";";
                for (UINT32 i = 0; i < m_inputCount; i++)
                {
                    ss << m_inputParams[i].Sn << ";";
                }
                ss << "0;";
                m_currentScopeCEPState = ss.str();

                if (uri != nullptr)
                {
                    m_lastCheckpointLogicalTime = m_lastCTITime;
                    m_lastCheckpointWallclockTime = now;
                    ss << uri->c_str();
                    m_lastScopeCEPCheckpointState = ss.str();
                }
                char nowString[256];
                now.ToString(nowString, sizeof(nowString));
                if (uri != nullptr)
                {
                    printf("\n%s:Checkpoint Status %s\n", nowString, m_lastScopeCEPCheckpointState.c_str());
                }
                else
                {
                    printf("\n%s:CTI Status %s\n", nowString, m_currentScopeCEPState.c_str());
                }
            }
        }

        BinaryInputStream* GenerateScopeCEPCheckpointFromInitialState() 
        {
            if (!m_startScopeCEPState.empty())
            {
                char c;
                istringstream ss(m_startScopeCEPState);
                __int64 ts;
                ss >> ts;
                m_lastCTITime = ScopeDateTime::FromFileTime(ts, true);
                m_startCTITime = m_lastCTITime;
                ss >> c;
                ss >> m_seqNumber;
                ss >> c;
                UINT inputCount;
                ss >> inputCount;
                ss >> c;
                SCOPE_ASSERT(inputCount == m_inputCount);
                for (UINT32 i = 0; i < m_inputCount; i++)
                {
                    ss >> (UINT64)m_inputParams[i].Sn;
                    ss >> c;
                }

                UINT skipRecoveryInputCount;
                ss >> skipRecoveryInputCount;
                ss >> c;
                SCOPE_ASSERT(skipRecoveryInputCount <= m_inputCount);
                for (UINT32 i = 0; i < skipRecoveryInputCount; i++)
                {
                    UINT skippedInputIndex;
                    ss >> skippedInputIndex;
                    ss >> c;
                    SCOPE_ASSERT(skippedInputIndex < m_inputCount);
                    m_inputParams[skippedInputIndex].NeedSkipInputRecovery = true;
                }

                string uri = m_startScopeCEPState.substr(ss.tellg());
                printf("loading checkpoint from %s\n", uri.c_str());
                IOManager::GetGlobal()->AddInputStream(uri, uri);

                if (!m_alloc)
                {
                    m_alloc = new IncrementalAllocator(MemoryManager::x_maxMemSize, "ScopeCEPCheckpointManager");
                }

                InputFileInfo input;
                input.inputFileName = uri;
                BinaryInputStream* checkpoint = new BinaryInputStream(input, m_alloc, 0x400000, 2);

                checkpoint->Init();

                return checkpoint;
            }
            else
            {
                return NULL;
            }
        }

        UINT64 IncrementSeqNumber()
        {
            return InterlockedIncrement64((volatile INT64 *)&m_seqNumber);
        }

        UINT64 GetCurrentSeqNumber()
        {
            return m_seqNumber;
        }

        void SetSeqNumber (UINT64 sn) { m_seqNumber = sn; }

        void UpdateLastCTITime(const ScopeDateTime& ts) { m_lastCTITime = ts; }

        void CreateTrackingSNArrayForExtract(UINT count)
        {
            SCOPE_ASSERT(m_inputParams == nullptr);
            if (count > 0)
            {
                m_inputParams = new StreamingInputParams[count];
            }
            m_inputCount = count;
        }

        StreamingInputParams* GetInputParameter(UINT index)
        {
            SCOPE_ASSERT(index < m_inputCount);
            return m_inputParams + index;
        }

        bool IsWorthyToDoCheckpoint(const ScopeDateTime& cti) 
        { 
            ScopeDateTime now = ScopeDateTime::Now();
            bool result = (m_lastCheckpointWallclockTime.AddSeconds(m_minimalSecondsCheckpointWallclockInterval) <= now ||
                           m_lastCheckpointLogicalTime.AddSeconds(m_minimalSecondsCheckpointLogicalTimeInterval) <= cti);
            if (!result)
            {
                printf("Skip CTIWithCheckpoint %I64u at %I64u, last Checkpoint (time:%I64u, CTI %I64u)\r\n",
                    cti.ToFileTime(true), 
                    now.ToFileTime(true),
                    m_lastCheckpointWallclockTime.ToFileTime(true),
                    m_lastCheckpointLogicalTime.ToFileTime(true));
            }
            return result;
        }

        template<class Operator>
        void InitiateCheckPointChain(Operator* outputer)
        {
            BinaryOutputStream* checkpoint = InitiateCheckPointChainInternal(outputer);
            checkpoint->Finish();
            checkpoint->Close();
            delete checkpoint;
        }

        template<class Operator>
        BinaryOutputStream* InitiateCheckPointChainInternal(Operator* outputer)
        {      
            std::string uri = GenerateCheckpointUri();
            IOManager::GetGlobal()->AddOutputStream(uri, uri, "", ScopeTimeSpan::TicksPerWeek);
            BinaryOutputStream* checkpoint = new BinaryOutputStream(uri, 0x400000, 2);
            checkpoint->Init();
            outputer->DoScopeCEPCheckpoint(*checkpoint);
            checkpoint->Flush(true);
            UpdateCurrentScopeCEPState(&uri);

            return checkpoint;
        }
    };
#pragma pack(pop)

    class StreamingOutputChannel
    {
    public:
        ~StreamingOutputChannel(){}
        virtual void SetAllowDuplicateRecord(bool allow) = 0;
        virtual bool TryAdvanceCTI(const ScopeDateTime& currentCTI, const ScopeDateTime& finalOutputMinCTI, bool hasBufferedData) = 0;
    };
    
    inline void CosmosInput::SaveState(BinaryOutputStream& output, UINT64 position)
    {
        std::string stateString;
        m_scanner->SaveState(stateString, position);
        output.Write((unsigned int)stateString.size() + 1); //always include null terminator
        output.Write(stateString.c_str(), (unsigned int)stateString.size() + 1);
    }

    inline void CosmosInput::LoadState(BinaryInputStream& input)
    {
        unsigned int length;
        input.Read(length); 
        m_recoverState.resize(length - 1);
        input.Read(&m_recoverState[0], length);
    }

    inline void CosmosInput::ClearState()
    {
        m_recoverState.clear();
    }

    inline void CosmosOutput::SaveState(BinaryOutputStream& output)
    {
        std::string stateString;
        m_scanner->SaveState(stateString, 0);
        output.Write((unsigned int)stateString.size() + 1); //always include null terminator
        output.Write(stateString.c_str(), (unsigned int)stateString.size() + 1);
    }

    inline void CosmosOutput::LoadState(BinaryInputStream& input)
    {
        unsigned int length;
        input.Read(length); 
        m_recoverState.resize(length - 1);
        input.Read(&m_recoverState[0], length);
    }
} // namespace ScopeEngine

extern ScopeEngine::ScopeCEPCheckpointManager* g_scopeCEPCheckpointManager;
