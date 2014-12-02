#pragma once

// Windows api include and debug assert
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <psapi.h>

#include <stdlib.h>
#include <memory>
#include <queue>
#include <map>
#include <intrin.h>

namespace ScopeEngine
{
    //
    // Class to store and track execution stats.
    // This is for per operator tracking as well as tracking I/O timing.
    //
    class SCOPE_RUNTIME_API ExecutionStats
    {
        // Return the frequency of the timestamp
        static LONGLONG GetFrequencyTS()
        {
            LARGE_INTEGER tps;
            QueryPerformanceFrequency(&tps);

            return tps.QuadPart;
        }

        static LONGLONG GetTimestamp()
        {
            LARGE_INTEGER ts;
            QueryPerformanceCounter(&ts);

            return ts.QuadPart;
        }

    public:
        ExecutionStats():m_inclusiveTime(0), m_beginTime(0), m_rowCount(0), m_callCount(0)
        {
            m_frequency = GetFrequencyTS();
        }

#if defined(SCOPE_NOSTATS)
        void BeginInclusive()
        {
        }

        void EndInclusive()
        {
        }

        void IncreaseRowCount(ULONGLONG)
        {
        }
#else
        // begin inclusive timer
        void BeginInclusive()
        {
            m_beginTime = GetTimestamp();
        }

        // end inclusive timer
        void EndInclusive()
        {
            m_inclusiveTime += GetTimestamp() - m_beginTime;
            ++m_callCount;
        }

        // increase current operator's produced row count
        void IncreaseRowCount(ULONGLONG count)
        {
            m_rowCount += count;
        }
#endif

        LONGLONG GetRawTicks()
        {
            return m_inclusiveTime;
        }

        // return inclusive time in ms
        LONGLONG GetInclusiveTimeMillisecond()
        {
            // Need to adjust with tick overhead
            // ULONGLONG adjustedTicks = m_inclusiveTime - m_callCount * FTGetTickOverhead();
            return m_inclusiveTime * 1000 / m_frequency;
        }

        ULONGLONG GetRowCount()
        {
            return m_rowCount;
        }

    private:
        LONGLONG m_inclusiveTime;
        LONGLONG m_beginTime;
        ULONGLONG m_rowCount;
        ULONGLONG m_callCount;
        LONGLONG m_frequency;
    };

    //
    // Wrapper to track cpu ticks automatically in a scope.
    //
    class SCOPE_RUNTIME_API AutoExecStats
    {
    public:
        AutoExecStats(ExecutionStats * p)
        {
            m_pStats = p;
            p->BeginInclusive();
        }

        ~AutoExecStats()
        {
            m_pStats->EndInclusive();
        }

        void IncreaseRowCount(ULONGLONG count)
        {
            m_pStats->IncreaseRowCount(count);
        }
    private:
        ExecutionStats * m_pStats;   // stats object
    };

    //
    // RAII for the CRITICAL_SECTION
    //
    class AutoCriticalSection
    {
    public:
        AutoCriticalSection(CRITICAL_SECTION* pCS, bool enterCriticalSection = true) : m_pCS(pCS)
        {
            if (enterCriticalSection)
            {
                EnterCriticalSection(m_pCS);
            }
        }

        ~AutoCriticalSection()
        {
            LeaveCriticalSection(m_pCS);
        }

    private:
        CRITICAL_SECTION* m_pCS;
    };

    static inline void DeleteAutoFlushTimer(HANDLE handle)
    {
        if (handle != INVALID_HANDLE_VALUE)
        {
            DeleteTimerQueueTimer(NULL, handle, INVALID_HANDLE_VALUE); 
        }
    }

    //
    // Specialization for the case when AutoRowArray stores objects
    //
    template<class Schema>
    class AutoRowArrayHelper
    {
    public:
        static void DestroyRow(Schema & p)
        {
            // http://msdn.microsoft.com/en-us/library/26kb9fy0.aspx
            // C4100 can also be issued when code calls a destructor on a otherwise unreferenced parameter of primitive type. This is a limitation of the Visual C++ compiler. 
#pragma warning(suppress: 4100)
            (&p)->~Schema();
        }

        static void ConstructRow(Schema & buf, Schema * row, IncrementalAllocator & alloc)
        {
            // call in place new operator to make a deep copy of the row object
            new ((char*)&buf) Schema(*row, &alloc);
        }
    };

    //
    // Specialization for the case when AutoRowArray stores pointers to objects
    //
    template<class Schema>
    class AutoRowArrayHelper<Schema*>
    {
    public:
        static void DestroyRow(Schema * & p)
        {
            // http://msdn.microsoft.com/en-us/library/26kb9fy0.aspx
            // C4100 can also be issued when code calls a destructor on a otherwise unreferenced parameter of primitive type. This is a limitation of the Visual C++ compiler. 
#pragma warning(suppress: 4100)
            p->~Schema();
        }

        static void ConstructRow(Schema * & buf, Schema ** row, IncrementalAllocator & alloc)
        {
            // if Schema is pointer, we need to allocate memory for schema object and do deep copy.
            buf = (Schema *)alloc.Allocate(sizeof(Schema));
            new ((char*)buf) Schema(**row, &alloc);
        }
    };

    //
    // A wrapper class to implement auto grow array for row object.
    // If Schema is a pointer type we deep copy the object that it points to.
    // It relies on exclusive usage of the IncrementalAllocator and the allocator allocating memory continuously.
    //
    template<typename Schema>
    class AutoRowArray
    {
        IncrementalAllocator      m_allocator;// allocator
        IncrementalAllocator      m_blobAllocator;// allocator for blob
        Schema     *   m_buffer;   // buffer start point
        SIZE_T         m_index;    // next write position
        SIZE_T         m_memUsed;  // memory used by the m_buffer.
        SIZE_T         m_size;     // total size of memory that committed
        SIZE_T         m_limit;    // upbound of rows can be stored

        static const SIZE_T x_maxNumOfRow = 1000000;
        static const SIZE_T x_maxMemorySize = 2147483648;  // 2G per buckets
        static const SIZE_T x_allocationUnit = 1 << 16; // 64k

        // call destructor of each Row and reset memory
        void DestroyRows()
        {
            //call destructor of each row.
            for (SIZE_T i=0; i<m_index;i++)
            {
                // AutoRowArray will allocate the whole buffer as char * to improve new performance.
                // Each element in the array will be initialized using in place new operator.
                // When we destroy the array we need to make sure Schema class destructor is called.
                // Otherwise, resource owned by Schema object may not be released.
                AutoRowArrayHelper<Schema>::DestroyRow(m_buffer[i]);
            }

            m_index = 0;
        }

        void Init(const string& ownerName, SIZE_T maxRows, SIZE_T maxMemory)
        {
            m_limit = maxRows;
            m_index = 0;

            // add some extra buffer for the row array because the allocation may not aligned with allocation Unit size
            m_allocator.Init(RoundUp_Size(sizeof(Schema)*maxRows+x_allocationUnit*2), ownerName + "_AutoRowArray");

            SIZE_T blobSize = max<__int64>(maxMemory - m_allocator.GetMaxSize(), 0);
            // if max memory limit is not enough for the schema object, we just allocate bloballocator same as row allocator.
            if (blobSize == 0)
            {
                blobSize = m_allocator.GetMaxSize();
            }
            m_blobAllocator.Init(blobSize, ownerName + "_AutoRowArray_Blob");

            m_buffer = (Schema *)(m_allocator.Buffer());
            m_size = 0;
            m_memUsed = 0;
        }

    public:
        enum MemorySizeCategory
        {
            SmallMem = 100,  // 1%  of the limit
            MediumMem = 5,   // 20% of the limit
            LargeMem = 1     // 100% of the limit
        };

        enum CountSizeCategory
        {
            ExtraSmall = 10000,  // 0.01%  of the limit
            Small = 1000,  // 0.1%  of the limit
            Medium = 20,   // 5% of the limit
            Large = 1     // 100% of the limit
        };

        AutoRowArray(const char* ownerName, SIZE_T maxRows, SIZE_T maxMemory)
        {
            Init(string(ownerName), maxRows, maxMemory);
        }

        AutoRowArray(const char* ownerName, CountSizeCategory sizeCat = Large, MemorySizeCategory memCat = LargeMem)
        {
            Init(string(ownerName), x_maxNumOfRow / sizeCat, x_maxMemorySize / memCat);
        }

        ~AutoRowArray()
        {
            DestroyRows();
        }

        //Reset the AutoRowArray
        template <class ReclaimPolicy>
        FORCE_INLINE void Reset()
        {
            DestroyRows();

            m_allocator.Reset<ReclaimPolicy>();
            m_blobAllocator.Reset<ReclaimPolicy>();

            m_buffer = (Schema *)(m_allocator.Buffer());
            m_size = 0;
            m_memUsed = 0;
        }

        // Reset with default reclaim policy
        FORCE_INLINE void Reset()
        {
            Reset<IncrementalAllocator::AmortizeMemoryAllocationPolicy>();
        }

        // Add a row into the cache. If we run out of memory, return false.
        bool AddRow(Schema & row)
        {
            // if there is not enough space to hold a row
            if (m_memUsed + sizeof(Schema) > m_size)
            {
                // round up allocation to x_allocationUnit block size.
                SIZE_T commitSize = ((m_memUsed + sizeof(Schema) - m_size + x_allocationUnit - 1) >> 16) * x_allocationUnit ;

                // we will not run out of memory here since the limit is precalculated according to schema object size + 2*x_allocationUnit.
                char * tmp = m_allocator.Allocate(commitSize);

                // assert the allocation is continuous
                SCOPE_ASSERT(tmp == (char *)m_buffer+m_size);

                m_size += commitSize;
            }

            bool returnValue = true;

            try
            {
                AutoRowArrayHelper<Schema>::ConstructRow(m_buffer[m_index], &row, m_blobAllocator);
                m_blobAllocator.EndOfRowUpdateStats();

                // bump up index only if we sucessfully copy the row.
                m_index++;
                m_memUsed += sizeof(Schema);
            }
            catch (RuntimeMemoryException & )
            {
                // in case we hit runtime OOM exception
                returnValue = false;
            }

            return returnValue;
        }

        // Amount of occupied memory
        SIZE_T MemorySize() const
        {
            return m_allocator.GetSize() + m_blobAllocator.GetSize();
        }

        // Current size
        SIZE_T Size() const
        {
            return m_index;
        }

        // Current limit
        SIZE_T Limit() const
        {
            return m_limit;
        }

        // array accessor to get to the buffer element
        Schema & operator[] (SIZE_T i) const
        {
            return m_buffer[i];
        }

        Schema * Begin() const
        {
            return m_buffer;
        }

        Schema * End() const
        {
            return &(m_buffer[m_index]);
        }

        // whether autorowarray reach it's capacity
        // TODO, add throttling based on overall memory consumption.
        bool FFull() const
        {
            return m_index >= m_limit;
        }

        void WriteRuntimeStats(TreeNode & root) const
        {
            auto & node = root.AddElement("AutoRowArray");
			m_allocator.WriteRuntimeStats(node);
			m_blobAllocator.WriteRuntimeStats(node, sizeof(Schema));
        }
    };

    //
    // An auto grow array for fixed-count circular queue of rows.
    // The array starts out very lightweight and only commits memory needed.
    // It stores rows inside its buffer, contiguously, up to the limit.
    // It does not own its rows, so it is not responsible for deletion.
    // It relies on the InOrderAllocator for blob storage.
    //
    template<typename Schema>
    class RowRingBufferInternal
    {
    private:
        template<typename Schema> friend class RowRingBuffer;

        IncrementalAllocator      m_allocator;// allocator
        InOrderAllocator      m_blobAllocator;// allocator for blob
        Schema     *  m_buffer;   // buffer start point
        SIZE_T         m_index;    // next write position
        SIZE_T         m_memUsed;  // memory used by the m_buffer.
        SIZE_T         m_size;     // total size of memory that committed
        SIZE_T         m_limit;    // upbound of rows can be stored

        static const SIZE_T x_allocationUnit = 1 << 16; // 64k

        // RowRingBufferInternal will allocate the whole buffer as char * to improve new performance.
        // Each element in the array will be initialized using in place new operator.
        void DestroyRow(SIZE_T i)
        {
            // http://msdn.microsoft.com/en-us/library/26kb9fy0.aspx
            // C4100 can also be issued when code calls a destructor on a otherwise unreferenced parameter of primitive type. This is a limitation of the Visual C++ compiler. 
#pragma warning(suppress: 4100)
            (&m_buffer[i])->~Schema();
        }

        // Single row in place new
        void ConstructRow(SIZE_T i, Schema& row)
        {
            if (0 == i)
            {
                // When maxRows queue insertions have occurred, it is safe to dispose of the previous maxRows rows.
                m_blobAllocator.AdvanceEpoch();
            }

            // Get the right IncrementalAllocator from m_blobAllocator and use it.
            new ((char*)&m_buffer[i]) Schema(row, &m_blobAllocator.CurrentAllocator());
            m_blobAllocator.CurrentAllocator().EndOfRowUpdateStats();
        }

    public:
        RowRingBufferInternal(SIZE_T maxRows, SIZE_T maxMemory, const string& ownerName)
        {
            m_limit = maxRows;
            m_index = 0;

            // add some extra buffer for the row array because the allocation may not aligned with allocation Unit size
            m_allocator.Init(RoundUp_Size(sizeof(Schema)*maxRows+x_allocationUnit*2), ownerName);

            SIZE_T blobSize = max<__int64>(maxMemory - m_allocator.GetMaxSize(), 0);
            // if max memory limit is not enough for the schema object, we just allocate bloballocator same as row allocator.
            if (blobSize == 0)
            {
                blobSize = m_allocator.GetMaxSize();
            }
            m_blobAllocator.Init(blobSize, ownerName);

            m_buffer = (Schema *)(m_allocator.Allocate(x_allocationUnit));
            m_size = x_allocationUnit;
            m_memUsed = 0;
        }

        void AddRow(Schema & row)
        {
            SCOPE_ASSERT(!FFull());

            // if there is not enough space to hold a row
            if (m_memUsed + sizeof(Schema) > m_size)
            {
                // round up allocation to x_allocationUnit block size.
                SIZE_T commitSize = ((m_memUsed + sizeof(Schema) - m_size + x_allocationUnit - 1) >> 16) * x_allocationUnit ;

                // we will not run out of memory here since the limit is precalculated according to schema object size + 2*x_allocationUnit.
                char * tmp = m_allocator.Allocate(commitSize);

                // assert the allocation is continuous
                SCOPE_ASSERT(tmp == (char *)m_buffer+m_size);

                m_size += commitSize;
            }

            ConstructRow(m_index, row);
            m_index++;
            m_memUsed += sizeof(Schema);
        }

        SIZE_T Size() const
        {
            return m_index;
        }

        SIZE_T Limit() const
        {
            return m_limit;
        }

        Schema & operator[] (SIZE_T i) const
        {
            return m_buffer[i];
        }

        bool FFull() const
        {
            return m_index >= m_limit;
        }

        void WriteRuntimeStats(TreeNode & root) const
        {
            m_allocator.WriteRuntimeStats(root);
            m_blobAllocator.WriteRuntimeStats(root);
        }
    };

    //
    // Ring buffer of rows.  Fixed size, with add and remove operations.
    // Uses an array (RowRingBufferInternal) and controls row lifetime in that array.
    //
    // Not thread-safe.
    //
    template<typename Schema>
    class RowRingBuffer
    {
    private:
        static const SIZE_T x_maxMemorySizeForRingBuffer = 2147483648;  // Just use the whole 2GiB for now

        // Dynamic array storage with in-order allocation
        RowRingBufferInternal<Schema> internal;

        // position of front of queue
        // E.g. after first 5 adds to a size 20 ring buffer, the front is at position 0 and the back is at position 5.
        SIZE_T m_front;

        // count of queue
        // Need to track this because it can shrink while RowRingBufferInternal's count does not.
        SIZE_T m_count;

    private:
        SIZE_T Limit() const { return internal.Limit(); }

        SIZE_T Back() const { return RelativePosition(Count()); }

        SIZE_T RelativePosition(SIZE_T i) const
        {
            SCOPE_ASSERT(i <= Count());
            return (i + m_front) % Limit();
        }

    public:
        RowRingBuffer(SIZE_T maxRows, const string& ownerName) : internal
            (
            maxRows,
            x_maxMemorySizeForRingBuffer,
            ownerName + "_RowRingBuffer"
            ), m_front(0), m_count(0)
        {
            SCOPE_ASSERT(maxRows > 0);
        }

        ~RowRingBuffer()
        {
            Reset();
        }

        SIZE_T Count() const { return m_count; }
        SIZE_T FFull() const { return Limit() <= Count(); }
        SIZE_T FValid() const { return Limit() >= Count() && internal.Size() >= Count(); }
        SIZE_T FEmpty() const { return 0 == Count(); }

        // Put a row into an empty slot in the ring buffer.
        void AddRow(Schema & row)
        {
            if (FFull())
            {
                throw new RuntimeException(E_SYSTEM_ERROR, "Attempting to add to full ring buffer.");
            }

            if (!internal.FFull())
            {
                // Loading array initially.  The array becomes full
                // after maxRows inserts, regardless of deletes.
                internal.AddRow(row);
            }
            else
            {
                internal.ConstructRow(Back(), row);
            }

            ++m_count;
            SCOPE_ASSERT(FValid());
        }

        void RemoveRow()
        {
            internal.DestroyRow(m_front);
            m_front = RelativePosition(1);
            --m_count;
            SCOPE_ASSERT(m_front < Limit());
        }

        // Access a row, indexing from m_front.
        Schema & operator[](SIZE_T i) const
        {
            SCOPE_ASSERT(i < Count());
            return internal[RelativePosition(i)];
        }

        void Reset()
        {
            while (Count())
            {
                RemoveRow();
            }
        }

        void WriteRuntimeStats(TreeNode & root) const
        {
            auto & node = root.AddElement("RowRingBuffer");
            internal.WriteRuntimeStats(node);
        }
    };

    //
    // Concurrent queue for the ParallelUnionAll
    //
    template<typename Schema>
    class ConcurrentBatchQueue
    {
    private:
        CONDITION_VARIABLE    m_bufFull;   // condition variable used for producer to wait on full buffer
        CONDITION_VARIABLE    m_bufEmpty;   // condition variable used for consumer to wait on empty buffer
        CRITICAL_SECTION    m_lock;      // lock to syncronize queue access
        CRITICAL_SECTION    m_freequeuelock;      // lock to syncronize freequeue access

        // we use small autorowarray as batch to manage memory
        typedef AutoRowArray<Schema> BatchType;

        // queue of produced batches
        queue<unique_ptr<BatchType>>         m_queue;

        // queue of free batches
        queue<unique_ptr<BatchType>>         m_freequeue;

        // number of producer for the queue.
        LONG                                 m_producerCnt;

        bool                                 m_cancelProducer;

        // we can hold at most 12 batch in the queue.
        static const ULONG x_queueLimit = 12;

    public:
        ConcurrentBatchQueue(int producerCnt) : m_producerCnt(producerCnt), m_cancelProducer(false)
        {
            InitializeConditionVariable (&m_bufFull);
            InitializeConditionVariable (&m_bufEmpty);
            InitializeCriticalSection (&m_lock);
            InitializeCriticalSection (&m_freequeuelock);
        }

        ~ConcurrentBatchQueue()
        {
            DeleteCriticalSection (&m_lock);
            DeleteCriticalSection (&m_freequeuelock);
        }

        void PushBatch(unique_ptr<BatchType> & batch)
        {
            AutoCriticalSection lock(&m_lock);

            // Queue is full, we need to wait for consumer to be done with some work first
            while (m_queue.size() >= x_queueLimit && !m_cancelProducer)
            {
                SleepConditionVariableCS(&m_bufFull, &m_lock, INFINITE);
            }

            m_queue.push(move(batch));

            // wake up consumer
            WakeConditionVariable(&m_bufEmpty);
        }

        bool Empty()
        {
            AutoCriticalSection lock(&m_lock);

            return m_queue.empty();
        }

        // Finish a producer.
        void Finish()
        {
            // decrease the producer count
            if (InterlockedDecrement(&m_producerCnt) == 0)
            {
                unique_ptr<BatchType> emptyBatch(new BatchType("ConcurrentBatchQueue_Empty", BatchType::Small, BatchType::MediumMem));

                // push an empty batch to indicate end of work
                PushBatch(emptyBatch);
            }
        }

        // Read one batch from queue.
        void Pop(unique_ptr<BatchType> & out)
        {
            AutoCriticalSection lock(&m_lock);

            // Queue is empty wait infinitely for new batch
            while (m_queue.empty())
            {
                SleepConditionVariableCS(&m_bufEmpty, &m_lock, INFINITE);
            }

            out = move(m_queue.front());

            m_queue.pop();

            // wake up producer
            WakeConditionVariable(&m_bufFull);
        }

        // Get a free batch from freequeue.
        bool GetFreeBatch(unique_ptr<BatchType> & out)
        {
            AutoCriticalSection lock(&m_freequeuelock);

            // Queue is not empty return a free batch
            if (!m_freequeue.empty())
            {
                out = move(m_freequeue.front());
                m_freequeue.pop();
                return true;
            }

            return false;
        }

        // Get a free batch from freequeue.
        void PutFreeBatch(unique_ptr<BatchType> & in)
        {
            AutoCriticalSection lock(&m_freequeuelock);

            // Push a batch to free queue
            m_freequeue.push(move(in));
        }

        // WakeUp producer in case the queue is full
        void CancelProducer()
        {
            AutoCriticalSection lock(&m_lock);

            m_cancelProducer = true;

            // wake up producer
            WakeConditionVariable(&m_bufFull);
        }
    };

    //
    // Class to cache data.
    // the key is std::string
    // this class is not responsible for cache entry memory release
    // therefore, the caller should handle memory clean up if entry type is a pointer.
    template<typename EntryType>
    class ConcurrentCache
    {
        typedef std::map<std::string, typename EntryType> CacheType;
        CacheType m_entryPool;
        CRITICAL_SECTION m_lock;      // lock to syncronize map access
        
    public:
        ConcurrentCache()
        {
            InitializeCriticalSection (&m_lock);
        }

        ~ConcurrentCache()
        {
            DeleteCriticalSection (&m_lock);
        }

        
        bool AddItem(std::string key, EntryType metadata)
        {
            AutoCriticalSection lock(&m_lock);
            return m_entryPool.insert(std::make_pair(key, metadata)).second;
        }
        
        bool RemoveItem(std::string key)
        {
            AutoCriticalSection lock(&m_lock);
            return m_entryPool.erase(key) != 0;
        }

        bool GetItem(std::string key, EntryType& metadata)
        {
            AutoCriticalSection lock(&m_lock);
            CacheType::iterator itr = m_entryPool.find(key);
            if (itr != m_entryPool.end())
            {
                metadata = itr->second;
                return true;
            }
            return false;
        }

        void Clear()
        {
            AutoCriticalSection lock(&m_lock);
            m_entryPool.clear();
        }
        
    };
    
    struct ScannerDeleter
    {
        void operator() (Scanner* scanner)
        {
            Scanner::DeleteScanner(scanner);
        }
    };

    inline double ScopeFmod(double a, double b)
    {
        return fmod(a, b);
    };

    inline double ScopeSqrt(double arg)
    {
        return sqrt(arg);
    };

    inline double ScopeCeiling(double arg)
    {
        return ceil(arg);
    };

    inline double ScopeFloor(double arg)
    {
        return floor(arg);
    };

    inline float ScopeFmod(float a, float b)
    {
        return fmodf(a, b);
    };

    // C++ methods for floating point modulo operations on nullable types. C# sematic is to return null if one of the
    // operatnds is null.
    inline NativeNullable<double> ScopeFmod(NativeNullable<double> a, NativeNullable<double> b)
    {
        return a.IsNull() ? a : (b.IsNull() ? b : scope_cast<NativeNullable<double>>(fmod(a.get(), b.get())));
    };

    inline NativeNullable<double> ScopeFmod(double a, NativeNullable<double> b)
    {
        return b.IsNull() ? b : scope_cast<NativeNullable<double>>(fmod(a, b.get()));
    };

    inline NativeNullable<double> ScopeFmod(NativeNullable<double> a, double b)
    {
        return a.IsNull() ? a : scope_cast<NativeNullable<double>>(fmod(a.get(), b));
    };

    inline NativeNullable<float> ScopeFmod(NativeNullable<float> a, NativeNullable<float> b)
    {
        return a.IsNull() ? a : (b.IsNull() ? b : scope_cast<NativeNullable<float>>(fmodf(a.get(), b.get())));
    };

    inline NativeNullable<float> ScopeFmod(float a, NativeNullable<float> b)
    {
        return b.IsNull() ? b : scope_cast<NativeNullable<float>>(fmodf(a, b.get()));
    };

    inline NativeNullable<float> ScopeFmod(NativeNullable<float> a, float b)
    {
        return a.IsNull() ? a : scope_cast<NativeNullable<float>>(fmodf(a.get(), b));
    };

    // trim from both ends
    inline std::string &trim(std::string &s)
    {
        if (s.empty())
        {
            return s;
        }
        
        auto firstItr = s.find_first_not_of(' ');
        if (firstItr == std::string::npos)
        {
            // the string only contains space
            s = "";
            return s;
        }
        
        auto lastItr = s.find_last_not_of(' ');
        if (firstItr != 0 || lastItr != s.length() - 1)
        {
            s = s.substr(firstItr, lastItr - firstItr + 1);
        }
        return s;
    }

    inline int GetPartitionIndex(std::string * argv, int argc, int partitionDimension)
    {
        SCOPE_ASSERT(partitionDimension >= 0);
        int partitionIndex = -1;
        vector<int> vertexIndices;
        // get partition index
        for (int i = 0; i < argc - 1; i++)
        {
            // it'll pass vertex index like -vertexIndex [1,2]
            // JM could pass empty array like -vertexIndex [] even it doesn't pass vertex index via this parameter
            if (argv[i].compare("-vertexIndex") == 0)
            {                
                string& str = argv[i+1];
                SCOPE_ASSERT(str.length() >= 2 && str[0] == '[' && str[str.length() - 1] == ']');
                char delimiter = ',';
                string::size_type pos = 1;
                string tmp;
                int idx = 1;
                for (idx = 1; idx < (str.length() - 1); idx++)
                {
                    if (str[idx] == delimiter)
                    {
                        tmp = str.substr(pos, idx - pos);
                        tmp = trim(tmp);
                        if (tmp.empty())
                        {
                            vertexIndices.push_back(-1);
                        }
                        else
                        {
                           int num = atoi(tmp.c_str());
                           SCOPE_ASSERT(num != 0 || tmp[0] == '0');
                           vertexIndices.push_back(num);
                        }
                        pos = idx + 1;
                    }
                }
                
                tmp = str.substr(pos, idx - pos);
                tmp = trim(tmp);
                if (tmp.empty())
                {
                    vertexIndices.push_back(-1);
                }
                else
                {
                   int num = atoi(tmp.c_str());
                   SCOPE_ASSERT(num != 0 || tmp[0] == '0');
                   vertexIndices.push_back(num);
                }
                i++;
            }
            else if (argv[i].compare("-partitionIndex") == 0)
            {
                partitionIndex = atoi(trim(argv[i+1]).c_str());
                SCOPE_ASSERT(partitionIndex != 0 || *(argv[i+1].c_str()) == '0');
                i++;
            }
        }

        for (int i = 0; i < vertexIndices.size(); i++)
        {
            if (vertexIndices[i] < 0)
            {
                vertexIndices[i] = partitionIndex;
            }
        }
        
        SCOPE_ASSERT(partitionDimension < vertexIndices.size());
        return vertexIndices[partitionDimension];
    };

    inline std::string GetChannelName(std::string inputFileName)
    {        
        // file name format is EntryName:ChannelName:input#
        // refer to AddInputChannel which constructs the file name
        string::size_type firstIdx = inputFileName.find_first_of(":");
        SCOPE_ASSERT(firstIdx != string::npos);
        string::size_type secondIdx = inputFileName.find_first_of(":", firstIdx + 1);
        SCOPE_ASSERT(firstIdx != string::npos);
        return inputFileName.substr(firstIdx + 1, secondIdx - firstIdx - 1);
    }

    class ScopeGuard
    {
    private:
        volatile long* m_pCount;

    public:
        ScopeGuard(volatile long* pCount) : m_pCount(pCount)
        {
            InterlockedIncrement(m_pCount);
        }

        ~ScopeGuard()
        {
            InterlockedDecrement(m_pCount);
        }
    };
} // namespace ScopeEngine
