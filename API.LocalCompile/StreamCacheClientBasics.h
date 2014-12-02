#pragma once
#include <stdint.h>


namespace StreamCache
{
    class ICacheBlock
    {
    public:
        virtual bool ReadOnly() = 0;
        virtual uint64_t GetBufferSize() = 0 ; 
        virtual uint8_t* GetBlockPtr() = 0 ;
        virtual uint64_t GetContentSize() = 0;
        virtual void SetContentSize(const uint64_t newSize) = 0;
        virtual uint32_t GetUserCacheFlags() = 0;
        virtual void SetUserCacheFlags(const uint32_t cacheFlags) = 0;

        // In case a cache entry was pinned for insert, the user may fail to fill this entry; in this case, Invalidate() is called to indicate to discard this entry.
        virtual void Invalidate() = 0;
        virtual bool Valid() = 0;
    };

    enum CacheKeyType
    {
        CKT_Invalid = 0,
        CKT_ScopeRuntime,
        CKT_BajaSSV2
    };

    enum CachePinModes
    {
        CPM_Read,
        CPM_Insert,
        CPM_ReadInsert
    };

    enum CacheAccessStates
    {
        CAS_Ok,
        CAS_Invalid, // exception raised
        CAS_NotExist, // not found in cache
        CAS_AlreadyExist, // already in cache, readable
        CAS_UnderConstruction //already in cache, but still under construction
    };

    struct CacheOptions
    {
        uint32_t m_options;

        void Init()
        {
            m_options = 0;
        }

        void Init(const CacheOptions& other)
        {
            m_options = other.m_options;
        };

        void Set(__in const uint32_t opt, __in const bool on)
        {
            if (on)
            {
                m_options |= opt;
            }
            else
            {
                m_options &= ~opt;
            }
        };

        bool Get(__in const uint32_t opt) const
        {
            return (m_options & opt) != 0;
        }
    };

    class ICacheKey
    {
    public:
        virtual CacheKeyType GetType() const = 0;
        virtual uint32_t GetHash() const = 0;

        // Implementation needs to cast "other" to the actual class const&.
        // If this and other have different CacheKeyType, return false
        virtual bool operator==(
            __in const ICacheKey& other) const = 0;

        // If 'buf' is too small to hold the serialized key, return false
        virtual bool Serialize(
            __out_bcount(bufSize) uint8_t* buf,
            __in const uint16_t bufSize,
            __out uint16_t& keySize) const = 0;
    };

    class ICachePool
    {
    public:
        virtual bool Init(
            __in const CacheOptions& initOptions,
            __in const uint64_t poolSize = 0) = 0;

        // std::exception may be raised in case of error
        virtual ICacheBlock* Pin(
            __in const ICacheKey& cacheKey,
            __in const uint64_t dataSize,
            __in const CachePinModes pinMode,
            __out CacheAccessStates& accessState,
            __in const bool waitable = true) = 0;

        // std::exception may be raised in case of error
        virtual void EndPin(
            __in ICacheBlock* cacheBlock,
            __in const bool discarding = false) = 0;

        virtual void RePin(
            __in ICacheBlock* cacheBlock,
            __in const CachePinModes pinMode) = 0;

        virtual bool Shutdown() = 0;
    };

}
