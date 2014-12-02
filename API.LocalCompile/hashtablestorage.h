#pragma once

#include "scopeengine.h"
#include "scopecontainers.h"
#include <string>
#include <vector>
#include <forward_list>

namespace ScopeEngine
{
    /**** ALLOCATOR ****/

    /*
     * HeapAllocator is STL compatible memory allocator
     * that uses Windows heap to dynamically handle its storage needs.
     */
    template <typename T>
    class HeapAllocator
    {
    public:
        typedef T value_type;
        typedef T* pointer;
        typedef T& reference;
        typedef const T* const_pointer;
        typedef const T& const_reference;

        template <typename U>
        struct rebind
        {
            typedef HeapAllocator<U> other;
        };

    private:
        HANDLE m_heap;

    public:
        HeapAllocator()
            : m_heap(NULL)
        {
        }

        HeapAllocator(HANDLE heap)
            : m_heap(heap)
        {
            SCOPE_ASSERT(m_heap != NULL);
        }

        HeapAllocator(const HeapAllocator& alloc)
            : m_heap(alloc.m_heap)
        {
        }

        template <typename U>
        HeapAllocator(const HeapAllocator<U>& alloc)
            : m_heap(alloc.m_heap)
        {
        }

        pointer allocate(SIZE_T n)
        {
            SIZE_T size = n * sizeof(value_type);
            void* memory = nullptr;

            memory = HeapAlloc(m_heap, 0, size);
            
            if (GetLastError() == ERROR_NOT_ENOUGH_MEMORY)
            {
                std::ostringstream message;
                message << "Could not allocate memory: ";
                message << size;
                throw RuntimeException(E_SYSTEM_HEAP_OUT_OF_MEMORY, message.str().c_str());
            }

            SCOPE_ASSERT(memory);

            return reinterpret_cast<pointer>(memory);
        }

        void deallocate(pointer p, SIZE_T /*n*/)
        {
            bool deallocated = HeapFree(m_heap, 0, p);

            SCOPE_ASSERT(deallocated);
        }

        void construct(pointer p, const T& t)
        {
            new ((void*)p) T(t);
        }

        // this cctor enables stl containers to use
        // move instead of copy when possible
        template<class U, class... Args>
		void construct(U* p, Args&&... args)
        {
		    new ((void*)p) U(std::forward<Args>(args)...);
		}

        void destroy(pointer p)
        {
            p->T::~T();
        }

        bool operator == (const HeapAllocator& other) const
        {
            return m_heap == other.m_heap;
        }
        
        bool operator != (const HeapAllocator& other) const
        {
            return !operator == (other);
        }

        template <class U> friend class HeapAllocator;
    };

    // Void specialization of HeapAllocator used for convenience.
    template <>
    class HeapAllocator<void>
    {
    public:
        typedef void value_type;
        typedef void* pointer;
        typedef const void* const_pointer;

        template <typename U>
        struct rebind
        {
            typedef HeapAllocator<U> other;
        };

    private:
        HANDLE m_heap;

    public:
        HeapAllocator()
            : m_heap(NULL)
        {
        }
        
        HeapAllocator(HANDLE heap)
            : m_heap(heap)
        {
            SCOPE_ASSERT(m_heap != NULL);
        }
        
        HeapAllocator(const HeapAllocator& alloc)
            : m_heap(alloc.m_heap)
        {
        }
        
        template <typename U>
        HeapAllocator(const HeapAllocator<U>& alloc)
            : m_heap(alloc.m_heap)
        {
        }
        
        bool operator == (const HeapAllocator& other) const
        {
            return m_heap == other.m_heap;
        }
        
        bool operator != (const HeapAllocator& other) const
        {
            return !operator == (other);
        }
        
        template <class U> friend class HeapAllocator;
    };

    typedef HeapAllocator<void> DefaultHeapAllocator;

    /*
     * Heap is a wrapper around Windows heap that manages
     * its construction and destruction.
     */
    class Heap
    {
    private:    
        HANDLE m_handle;

    public:
        Heap()
            : m_handle(HeapCreate(HEAP_NO_SERIALIZE, 0, 0))
        {
            SCOPE_ASSERT(m_handle != NULL);
        }

        const HANDLE Ptr() const 
        {
            return m_handle;
        }

        ~Heap()
        {
            HeapDestroy(m_handle);
        }
    };


    /*************************************************/
    /** STL compartible IncrementalAllocator wrapper */

    template <typename T>
    class STLIncrementalAllocator
    {
    public:
        typedef T value_type;
        typedef T* pointer;
        typedef T& reference;
        typedef const T* const_pointer;
        typedef const T& const_reference;

        template <typename U>
        struct rebind
        {
            typedef STLIncrementalAllocator<U> other;
        };

    private:
        IncrementalAllocator* m_alloc;

    public:
        STLIncrementalAllocator()
            : m_alloc(nullptr)
        {
        }

        STLIncrementalAllocator(IncrementalAllocator* alloc)
            : m_alloc(alloc)
        {
        }

        STLIncrementalAllocator(const STLIncrementalAllocator& other)
            : m_alloc(other.m_alloc)
        {
        }

        template <typename U>
        STLIncrementalAllocator(const STLIncrementalAllocator<U>& other)
            : m_alloc(other.m_alloc)
        {
        }

        pointer allocate(SIZE_T n)
        {
            SIZE_T size = n * sizeof(value_type);
            char* memory = nullptr;

            memory = m_alloc->Allocate(size);

            SCOPE_ASSERT(memory);

            return reinterpret_cast<pointer>(memory);
        }

        void deallocate(pointer p, SIZE_T /*n*/)
        {
            SCOPE_ASSERT(false);
        }

        void construct(pointer p, const T& t)
        {
            new ((void*)p) T(t);
        }

        void destroy(pointer p)
        {
            p->T::~T();
        }

        bool operator == (const STLIncrementalAllocator& other) const
        {
            return m_alloc == other.m_alloc;
        }
        
        bool operator != (const STLIncrementalAllocator& other) const
        {
            return !operator == (other);
        }

        template <class U> friend class STLIncrementalAllocator;
    };
    
    // Void specialization of STLIncrementalAllocator used for convenience.
    template <>
    class STLIncrementalAllocator<void>
    {
    public:
        typedef void value_type;
        typedef void* pointer;
        typedef const void* const_pointer;

        template <typename U>
        struct rebind
        {
            typedef STLIncrementalAllocator<U> other;
        };

    private:
        IncrementalAllocator* m_alloc;

    public:
        STLIncrementalAllocator()
            : m_alloc(nullptr)
        {
        }

        STLIncrementalAllocator(IncrementalAllocator* alloc)
            : m_alloc(alloc)
        {
        }

        STLIncrementalAllocator(const STLIncrementalAllocator& other)
            : m_alloc(other.m_alloc)
        {
        }

        template <typename U>
        STLIncrementalAllocator(const STLIncrementalAllocator<U>& other)
            : m_alloc(other.m_alloc)
        {
        }
        
        bool operator == (const STLIncrementalAllocator& other) const
        {
            return m_alloc == other.m_alloc;
        }
        
        bool operator != (const STLIncrementalAllocator& other) const
        {
            return !operator == (other);
        }
        
        template <class U> friend class STLIncrementalAllocator;
    };

    typedef STLIncrementalAllocator<void> DefaultSTLIncrementalAllocator;


    /*************************************************/
    

    /*
     * CollectDeepDataPtrs stores all FString/FBinary buffer pointers
     * to the provided ptrs array.
     
     * The function should be code gened
     * for schema classes that are used as Hashtable
     * KeySchema and ValueSchema. 
     *
     */
    void CollectDeepDataPtrs(const NullSchema& schema, std::vector<const char*>& ptrs);

    /*
     * MutableValueContainer is Hashtable storage unit. It stores 
     * a key and a value pointer. Supports update operation for the value.
     *
     * The class is not responsible for value memory management.
     * 
     *
     */
    template <typename KeySchema, typename ValueSchema, typename Allocator>
    class MutableValueContainer
    {
    public:
        typedef std::pair<KeySchema, ValueSchema*>                       KeyValue; 

    private:
        KeyValue      m_kv;

    public:
        template <typename Copier>
        MutableValueContainer(const KeySchema& key, ValueSchema* const value, Copier& copier, Allocator&)
            : m_kv(KeySchema(key, &copier), value)
        {
        }

        const KeySchema& Key() const
        {
            return m_kv.first;
        }

        KeyValue& Both()
        {
            return m_kv;
        }

        void Update(const ValueSchema& value)
        {
            *m_kv.second = value;
        }
    };


    /*
     * ListOfValuesContainer is Hashtable storage unit. It stores 
     * a key and a list of value pointer.
     *
     * The class is not responsible for memory management.
     */
    template <typename KeySchema, typename ValueSchema, typename Allocator>
    class ListOfValuesContainer
    {
    public:
        typedef          std::forward_list<const ValueSchema*, Allocator>   Values;
        typedef          std::pair<const KeySchema, Values>                 KeyValue;

    private:
        KeyValue     m_kv;

    public:
        template <typename Copier>
        ListOfValuesContainer(const KeySchema& key, const ValueSchema* value, Copier& copier, Allocator& alloc)
            : m_kv(KeySchema(key, &copier), Values(alloc))
        {
            Add(value);
        }
        
        const KeySchema& Key() const
        {
            return m_kv.first;
        }
        
        const KeyValue& Both() const
        {
            return m_kv;
        }

        void Add(const ValueSchema* value)
        {
            m_kv.second.push_front(value);
        }
    };
}

