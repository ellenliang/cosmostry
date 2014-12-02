#pragma once

// Enable this code once generated C++ files are split
 //#ifdef __cplusplus_cli
 //#error "Native scope operators should not be compiled with /clr"
 //#endif

#include <stdarg.h>

namespace ScopeEngine
{
    extern INLINE int BitNthMsf(ULONG mask)
    {
        unsigned long bIndex = 0;
        if (_BitScanReverse (&bIndex, mask))
            return bIndex;
        
        return -1;
    }

    extern INLINE int BitNthMsf(UINT64 mask)
    {
        unsigned long bIndex = 0;
        if (_BitScanReverse64 (&bIndex, mask))
            return bIndex;
        
        return -1;
    }

    extern INLINE USHORT ByteSwap(USHORT num)
    {
        return _byteswap_ushort(num);
    }

    extern INLINE ULONG ByteSwap(ULONG num)
    {
        return _byteswap_ulong(num);
    }

    extern INLINE std::wstring MakeBigString(const wchar_t *first, const wchar_t *second, ...)
    {
        va_list vl;
        const wchar_t *str = second;

        va_start( vl, second );

        std::wstring bigString = first;

        while(str != NULL)
        {
            bigString += str;
            str = va_arg( vl, const wchar_t *);
        }

        va_end(vl);

        return bigString;
    }

    extern INLINE std::string MakeBigString(const char *first, const char *second, ...)
    {
        va_list vl;
        const char *str = second;

        va_start( vl, second );

        std::string bigString = first;

        while(str != NULL)
        {
            bigString += str;
            str = va_arg( vl, const char *);
        }

        va_end(vl);

        return bigString;
    }

    //
    // This template allows creating vector of arbitrary length.
    // Be careful using this template as variable argument functions are not type safe.
    // NOTE that C++ compiler performs type promotion for some types (e.g. float -> double, char -> int)
    //      and if T is a class then copy constructor is not called during the call (just binary shallow copy)
    //
    template<class T>
    std::vector<T> MakeVector(size_t count, ...)
    {
        std::vector<T> vec(count);

        va_list vl;
    
        va_start(vl, count);

        for (size_t i = 0; i < count; ++i)
        {
            vec[i] = va_arg(vl, T);
        }
    
        va_end(vl);

        return vec;
    };
} // namespace ScopeEngine
