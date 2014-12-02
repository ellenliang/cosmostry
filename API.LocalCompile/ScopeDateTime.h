#pragma once

#ifdef SDT_TEST
#define SCOPE_ENGINE_API                         // when do test, nothing to import/export
#define SCOPE_ENGINE_MANAGED_API
#else
#ifndef SCOPE_ENGINE_API
#define SCOPE_ENGINE_API __declspec(dllimport)   // in user code, need to consume functions from ScopeEngine.dll
#endif
#ifndef SCOPE_ENGINE_MANAGED_API
#define SCOPE_ENGINE_MANAGED_API __declspec(dllimport)   // in user code, need to consume functions from ScopeEngineManaged.dll
#endif
#endif

#include <string>
#include <sstream>
#include <nmmintrin.h>

using namespace std;

namespace ScopeEngine
{
    class ScopeDateTime;
	class ScopeTimeSpan;

    // Bug #401369 and #401357 cause two restrictions here (though ScopeDateTime is a basic data type)
    //  1.    We can't include "windows.h", for quick compile
    //  2.    We can't have static objects, for unload dll
    // So we move both static members and functions calling WINAPI into ScopeEngine.dll. User code will call them via DLL import.   

    // Forward declarations
    // winapi related
    SCOPE_ENGINE_API LCID    GetCurrentThreadLocale();
    // The CLR ones so we can fast fallback if something happens
    SCOPE_ENGINE_MANAGED_API int ManagedScopeDateTimeToString(const ScopeDateTime & sdt, char * outbuf, int size, const char * dateTimeFormat);
    SCOPE_ENGINE_MANAGED_API bool ManagedTryParseScopeDateTime(const char * str, ScopeDateTime & out);
    SCOPE_ENGINE_MANAGED_API ScopeDateTime ManagedScopeDateTimeFromBinary(__int64 binary);
    SCOPE_ENGINE_MANAGED_API __int64 ManagedScopeDateTimeToBinary(const ScopeDateTime &in);

    // Structure used to represent DST(Daylight-Saving-Time) transition of one particular year
    typedef struct
    {
        __int64 start;  //start ticks of DST, in one particular year
        __int64 end;    //end ticks of DST, in one particular year
    } DstTransition, *PtrDstTransition;

	//structure used to represent _TIME_ZONE_INFORMATION transition of one particular year
	typedef struct NativeTimeZoneInfo
	{
		__int64 Bias;									// the current bias for lacal time translation on this computer in minutes.

		unsigned short standardDateWYear;				// year in one particular standard date
		unsigned short standardDateWMonth;				// month in one particular standard date
		unsigned short standardDateWDayOfWeek;			// day of week in one particular standard date
		unsigned short standardDateWDay;				// day in one particular standard date
		unsigned short standardDateWHour;				// hour in one particular standard date
		unsigned short standardDateWMinute;				// minute in one particular standard date
		unsigned short standardDateWSecond;				// second in one particular standard date
		unsigned short standardDateWMilliseconds;		// millisecond in one particular standard date

		__int64 StandardBias;							// the bias value to be used during local time translations that occur during starndard time.

		unsigned short dayLightDateWYear;				// year in one particular dayLight date 
		unsigned short dayLightDateWMonth;				// month in one particular dayLight date 
		unsigned short dayLightDateWDayOfWeek;			// day of week in one particular dayLight date 
		unsigned short dayLightDateWDay;				// day in one particular dayLight date 
		unsigned short dayLightDateWHour;				// hour in one particular dayLight date 
		unsigned short dayLightDateWMinute;				// minute in one particular dayLight date 
		unsigned short dayLightDateWSecond;				// second in one particular dayLight date 
		unsigned short dayLightDateWMilliseconds;		// milliSecond in one particular dayLight date 

		__int64 DaylightBias;							// the bias value to be used during local time translations that occur during day ligth saving time.
	} *PtrNativeTimeZoneInfo;


    // Struct used to pass in ScopeDateTime::Parse and ScopeDateTime::TryParse
    // Because the real input (from FString) is not null terminated
    struct DTStringInput
    {
        // now we use a std::string to add '\0', however it will introduce data copy
        // we should see if this hurt performance much. If not, we can just use string instead of DTStringInput
        // otherwise, we should just save the pointer and size here
        string str;
        DTStringInput(const char * c, unsigned int s)
        {
            str = string(c, s);
        }
    };

    // Provides culture-specific information about the format of date and time values.
    // Now only support subset (used by ScopeDateTime.ToString()) of en-US locale
    class ScopeDateTimeFormatInfo
    {
    public:
        enum MonthDateOrder
        {
            Order_Unknown = -1,
            Order_MD = 0,   // month before date in GeneralLongTimePattern
            Order_DM = 1    // date before month in GeneralLongTimePattern
        };

        // Current ScopeDateTime format info associated with current thread locale
        static ScopeDateTimeFormatInfo GetCurrentInfo()
        {
            ScopeDateTimeFormatInfo currentInfo;

            // get short date format
            currentInfo.ShortDatePattern = GetLocaleInfoString(LOCALE_SSHORTDATE);
            // get long date format
            currentInfo.LongDatePattern = GetLocaleInfoString(LOCALE_SLONGDATE);
            // get long time format
            currentInfo.LongTimePattern = GetLocaleInfoString(LOCALE_STIMEFORMAT);
            // get short time format ,
            currentInfo.ShortTimePattern = GetLocaleInfoString(LOCALE_SSHORTTIME);
            // get month/day format
            currentInfo.MonthDayPattern = GetLocaleInfoString(LOCALE_SMONTHDAY);
            // get year/month format
            currentInfo.YearMonthPattern = GetLocaleInfoString(LOCALE_SYEARMONTH);
            // get am designator
            currentInfo.AmDesignator = GetLocaleInfoString(LOCALE_S1159);
            // get pm designator
            currentInfo.PmDesignator = GetLocaleInfoString(LOCALE_S2359);

            // get round trip format, C# value is "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffffK", however "'" around "-" is useless
            currentInfo.RoundtripFormat = "yyyy-MM-ddTHH':'mm':'ss.fffffffK";
            // get RFC 1123 date format
            currentInfo.RFC1123DatePattern = "ddd, dd MMM yyyy HH':'mm':'ss 'GMT'";
            // get sortable format, C# value is "yyyy'-'MM'-'dd'T'HH':'mm':'ss", however "'" around "-" and "T" is useless
            currentInfo.SortableFormatPattern = "yyyy-MM-ddTHH':'mm':'ss";
            // get Universal time with sortable format, C# value is "yyyy'-'MM'-'dd HH':'mm':'ss'Z'", however "'" around "-" and "Z" is useless
            currentInfo.UniversalSortableFormatPattern = "yyyy-MM-dd HH':'mm':'ssZ";
            // era string, we hardcoded this to "A.D." since we support en-us only for now
            currentInfo.EraString = "A.D.";
            
            // get "U" format, universal full date format
            currentInfo.UniversalFullDatePattern = currentInfo.LongDatePattern + " " + currentInfo.LongTimePattern;
            // get "F" format (longdate + " " + longtime)
            currentInfo.FullLongTimePattern = currentInfo.LongDatePattern + " " + currentInfo.LongTimePattern;
            // get "f" format (longdate + " " + shorttime)
            currentInfo.FullShortTimePattern = currentInfo.LongDatePattern + " " + currentInfo.ShortTimePattern;
            // get "G" format (shortdate + " " + longtime)
            currentInfo.GeneralLongTimePattern = currentInfo.ShortDatePattern + " " + currentInfo.LongTimePattern;
            // get "g" format (shortdate + " " + shorttime)
            currentInfo.GeneralShortTimePattern = currentInfo.ShortDatePattern + " " + currentInfo.ShortTimePattern;
            // get time separator
            currentInfo.TimeSeparator = GetSeparator(currentInfo.LongTimePattern, "Hhms");
            // get date separator
            currentInfo.DateSeparator = GetSeparator(currentInfo.ShortDatePattern, "dyM");
            
            // identify if we cover it
            currentInfo.FallBackToClrHelper = (ScopeEngine::GetCurrentThreadLocale() != 1033);  // we only support en-us (locale id 1033)

            // check month date order in "G" format
            int dateIndex = (int) currentInfo.GeneralLongTimePattern.find('d');
            int MonthIndex = (int) currentInfo.GeneralLongTimePattern.find('M');

            if (dateIndex == -1 || MonthIndex == -1)
            {
                currentInfo.MdOrderInGeneralLongTimePattern = Order_Unknown;
            }
            else
            {
                currentInfo.MdOrderInGeneralLongTimePattern =  dateIndex < MonthIndex ? Order_DM : Order_MD;
            }

            for(int i = 0; i < 12; i++)
            {
                currentInfo.MonthNames[i] = GetLocaleInfoString(LOCALE_SMONTHNAME1 + i);
                currentInfo.AbbreviatedMonthNames[i] = GetLocaleInfoString(LOCALE_SABBREVMONTHNAME1 + i);
            }
            for(int i = 0; i < 7; i++)
            {
                currentInfo.DayOfWeekName[i] = GetLocaleInfoString(LOCALE_SDAYNAME1 + i);
                currentInfo.AbbreviatedDayOfWeekName[i] = GetLocaleInfoString(LOCALE_SABBREVDAYNAME1 + i);
            }
        
            return currentInfo;
        }

        static const int MAX_PATTERN_LEN = 160;
		// cut off year 
		// which means a two number year present years from 1930 to 2029.
		static const int TwoDigitYearMax = 2029;
		// the legal DateTime offset must be within 14 hours
		static const int MaxOffsetHours = 14;

        string LongDatePattern;
        string ShortDatePattern;
        string LongTimePattern;
        string ShortTimePattern;
        
        string FullShortTimePattern;
        string FullLongTimePattern;
        string GeneralLongTimePattern;
        string GeneralShortTimePattern;
        string RoundtripFormat;
        string RFC1123DatePattern;
        string SortableFormatPattern;
        string UniversalSortableFormatPattern;
        string UniversalFullDatePattern;
        string MonthDayPattern;
        string YearMonthPattern;

        MonthDateOrder MdOrderInGeneralLongTimePattern;
        string AmDesignator;
        string PmDesignator;
        string DateSeparator;
        string TimeSeparator;
        // for non en-us culture, we fallback to clr helper
        bool FallBackToClrHelper;

        // Month[0] is Jan.
        string MonthNames[12];
        string AbbreviatedMonthNames[12];
        // Day[0] is Monday
        string DayOfWeekName[7];
        string AbbreviatedDayOfWeekName[7];

        // Era string (i.e. A.D. for en-us)
        string EraString;

    private:

        SCOPE_ENGINE_API static string  GetLocaleInfoString(const LCTYPE LCType);

        // converted from CLR system.globalization.culturedata
        static string  GetSeparator(const string &format, string timeParts)
        {
            int index = IndexOfTimePart(format, 0, timeParts);

            if (index != -1)
            {
                // Found a time part, find out when it changes
                char cTimePart = format[index];
                int separatorStart = (int) format.find_first_not_of(cTimePart, index + 1);

                // Now we need to find the end of the separator
                if (separatorStart < (int) format.length())
                {
                    int separatorEnd = IndexOfTimePart(format, separatorStart, timeParts);
                    if (separatorEnd != -1)
                    {
                        // From [separatorStart, count) is our string, except we need to unescape
                        return UnescapeNlsString(format, separatorStart, separatorEnd - 1);
                    }
                }
            }

            return "";
        }

        // converted from CLR system.globalization.culturedata
        static int  IndexOfTimePart(const string &format, int startIndex, string timeParts)
        {
            if(startIndex < 0)
                throw std::invalid_argument("startIndex cannot be negative");
            if(timeParts.find('\'') != -1 || timeParts.find('\\') != -1)
                throw std::invalid_argument("timeParts cannot include quote characters");

            bool inQuote = false;
            int i = startIndex;
            while (i < (int) format.length())
            {
                // See if we have a time Part
                if (!inQuote && timeParts.find(format[i]) != -1)
                {
                    return i;
                }
                switch (format[i])
                {
                case '\\':
                    if (i + 1 < (int) format.length())
                    {
                        switch (format[i+1])
                        {
                        case '\'':
                        case '\\':
                            ++i;
                            break;
                        default:
                            break;
                        }
                    }
                    break;
                case '\'':
                    inQuote = !inQuote;
                    break;
                }

                ++i;
            }
            return -1;
        }

        // converted from CLR system.globalization.culturedata
        ////////////////////////////////////////////////////////////////////////////
        //
        // Unescape a NLS style quote string
        //
        // This removes single quotes:
        //      'fred' -> fred
        //      'fred -> fred
        //      fred' -> fred
        //      fred's -> freds
        //
        // This removes the first \ of escaped characters:
        //      fred\'s -> fred's
        //      a\\b -> a\b
        //      a\b -> ab
        //
        ////////////////////////////////////////////////////////////////////////////
        static string  UnescapeNlsString(const string &str, int start, int end)
        {
            if (start < 0)
                throw std::invalid_argument("start index can not be negative");
            if (end < 0)
                throw std::invalid_argument("end index can not be negative");

            string result;
            int strLen = (int) str.length();

            int i = start;
            while (i < strLen && i <= end)
            {
                switch (str[i])
                {
                case '\'':
                    if (result.empty())
                    {
                        result.append(str, start, i - start);
                    }
                    break;
                case '\\':
                    if (result.empty())
                    {
                        result.append(str, start, i - start);
                    }
                    ++i;
                    if (i < strLen)
                    {
                        result.push_back(str[i]);
                    }
                    break;
                default:
                    if (!result.empty())
                    {
                        result.append(str, start, i - start);
                    }
                    break;
                }

                ++i;
            }

            if (result.empty())
                return (str.substr(start, end - start + 1));

            return result;
        }
    };
	
    class ScopeDateTime
    {

        // The data is stored as an unsigned 64-bit integeter
        //   Bits 01-62: The value of 100-nanosecond ticks where 0 represents 1/1/0001 12:00am, up until the value
        //               12/31/9999 23:59:59.9999999
        //   Bits 63-64: A four-state value that describes the DateTimeKind value of the date time, with a 2nd
        //               value for the rare case where the date time is local, but is in an overlapped daylight
        //               savings time hour and it is in daylight savings time. This allows distinction of these
        //               otherwise ambiguous local times and prevents data loss when round tripping from Local to
        //               UTC time.
        unsigned __int64 m_time;

#define GET_TICKS(t)           ((t) & TicksMask)
#define GET_KIND(k)            ((k) & FlagsMask)
#define ENCODE(t,k)            ((t) | ((__int64)(k)) << KindShift)

        // Bitwise operation constants
        static const unsigned __int64 TicksMask              = 0x3FFFFFFFFFFFFFFFULL;
        static const unsigned __int64 FlagsMask              = 0xC000000000000000ULL;
        static const unsigned __int64 LocalMask              = 0x8000000000000000ULL;
        static const __int64          TicksCeiling           = 0x4000000000000000LL;
        static const unsigned __int64 KindUnspecified        = 0x0000000000000000ULL;
        static const unsigned __int64 KindUtc                = 0x4000000000000000ULL;
        static const unsigned __int64 KindLocal              = 0x8000000000000000ULL;
        static const unsigned __int64 KindLocalAmbiguousDst  = 0xC000000000000000ULL;
        static const int              KindShift              = 62;

        // Number of 100ns ticks per time unit
        static const __int64 TicksPerMillisecond = 10000;
        static const __int64 TicksPerSecond = TicksPerMillisecond * 1000;
        static const __int64 TicksPerMinute = TicksPerSecond * 60;
        static const __int64 TicksPerHour = TicksPerMinute * 60;
        static const __int64 TicksPerDay = TicksPerHour * 24;

        // Number of milliseconds per time unit
        static const int MillisPerSecond = 1000;
        static const int MillisPerMinute = MillisPerSecond * 60;
        static const int MillisPerHour = MillisPerMinute * 60;
        static const int MillisPerDay = MillisPerHour * 24;

        static const int DaysPerYear = 365;
        static const int DaysPer4Years = DaysPerYear * 4 + 1;       // 1461
        static const int DaysPer100Years = DaysPer4Years * 25 - 1;  // 36524
        static const int DaysPer400Years = DaysPer100Years * 4 + 1; // 146097
        static const int DaysTo10000Years = DaysPer400Years * 25 - 366;  // 3652059

        static const int MinYear = 0;
        static const int MaxYear = 9999;
        static const __int64 MinTicks = 0;
        static const __int64 MaxTicks = DaysTo10000Years * TicksPerDay - 1;
        static const __int64 MaxMillis = (__int64) DaysTo10000Years * MillisPerDay;

        SCOPE_ENGINE_API static const int DaysToMonth365[];
        SCOPE_ENGINE_API static const int DaysToMonth366[];

        static const int MaxToStringLen = 160;

        // the default format info if nothing is specified in ToString
        SCOPE_ENGINE_API static ScopeDateTimeFormatInfo CurrentDateTimeFormatInfo;

        // max number length when parsing a datetime string
        static const int MaxDateTimeNumberDigits = 8;

        // max fraction digits in seconds when format the datetime to string, i.e. "fffffff"
        // which is one tick granularity
        static const int MaxSecondsFractionDigits = 7;

        unsigned __int64 InternalKind() const
        {
            return GET_KIND(m_time);
        }

        ScopeDateTime Add(const double value, const int scale) const
        {
            __int64 millis = (__int64) (value * scale + (value >= 0 ? 0.5 : -0.5));
            if (millis <= -MaxMillis || millis >= MaxMillis)
                throw std::invalid_argument("argument out of range");

            return AddTicks(millis * TicksPerMillisecond);
        }

        friend class ScopeDateTimeParse;
		friend class ScopeCalendar;
		friend class __DTString;
		friend class TimeZoneInfo;
    public:

        // flag to force FromBinary, ToBinary, ToString, TryParse use CLR import method
        // change this config in ScopeDateTimeExt.h
        SCOPE_ENGINE_API static bool ForceManagedSerialization;

        // seperate flags for enable read/write respectively
        SCOPE_ENGINE_API static bool ForceManagedWrite;
        SCOPE_ENGINE_API static bool ForceManagedRead;

        // When something native code can't hadnle, just fail instead of falling back to CLR
        // This flag has lower priority than ForceManagedSerialization/ForceManagedWrite/ForceManagedRead
        // e.g. if ForceManagedSerialization == true and AllowManagedFallback == false, CLR routine will still be executed
        SCOPE_ENGINE_API static bool AllowManagedFallback;

        // simple static array for storing start&end ticks for interested years
        // to avoid frequent system calls which will degrade local-utc transition performance significantly
        // e.g. calling ToBinary() 1,000,000,000 times cost 150s wo cache, and 100s with cache
        // however not thread safe now and not support time zone changes in runtime
        // **NOTE** : time-zone change means manually change time-zone of the machine,
        // which does NOT include machine DST auto switch -- indeed we support this scenario
        SCOPE_ENGINE_API static PtrDstTransition DstTransitionDates[MaxYear - MinYear + 1];
		SCOPE_ENGINE_API static PtrNativeTimeZoneInfo NativeTZInfo[MaxYear - MinYear + 1];

        enum DateTimeKind
        {
            Unspecified = 0,
            Utc = 1,
            Local = 2
        };

        SCOPE_ENGINE_API static const ScopeDateTime MinValue;
        SCOPE_ENGINE_API static const ScopeDateTime MaxValue;

        // FileTime (starting from 1601-1-1)
        static const int DaysTo1601 = DaysPer400Years * 4; //584388
        static const __int64 FileTimeOffset = DaysTo1601 * TicksPerDay;

#ifdef SDT_TEST
        // place holder for MDA (managed debugging assistant), testing purpose only
        SCOPE_ENGINE_API static bool MDA_InvalidFormatForUtc;
#endif

        ScopeDateTime():m_time(0)
        {
        }

        ScopeDateTime(const ScopeDateTime & c):m_time(c.m_time)
        {
        }

        explicit ScopeDateTime(const __int64 ticks)
        {
            if (ticks < MinTicks || ticks > MaxTicks)
                throw std::invalid_argument("ticks out of supporting range year 0~9999");
            m_time = ENCODE(ticks, Unspecified);
        }

        ScopeDateTime(const __int64 ticks, const DateTimeKind kind)
        {
            if (ticks < MinTicks || ticks > MaxTicks)
                throw std::invalid_argument("ticks out of supporting range year 0~9999");
            if (kind < Unspecified || kind > Local)
                throw std::invalid_argument("invalid datetime kind");
            m_time = ENCODE(ticks, kind);
        }

        // TODO: this should be protected/friend (expose now for test purpose)
        // isAmbiguousDst -- indicate if is the first occurance of the ambiguous hour (the one in DST)
        ScopeDateTime(const __int64 ticks, const DateTimeKind kind, const bool isAmbiguousDst)
        {
            if (ticks < MinTicks || ticks > MaxTicks)
                throw std::invalid_argument("ticks out of supporting range year 0~9999");
            if (kind != Local)
                throw std::invalid_argument("constructor for local kind only");
            m_time = (unsigned __int64) ticks | (isAmbiguousDst ? KindLocalAmbiguousDst : KindLocal);
        }

        ScopeDateTime(int year, int month, int day, int hour, int minute, int second)
        {
            m_time = (unsigned __int64) (DateToTicks(year, month, day) + TimeToTicks(hour, minute, second));
        }

        // week -- week of month (5 == last week of month)
        // dayofweek -- day of week (0 == Sunday)
        ScopeDateTime(int year, int month, int week, int dayofweek, int hour, int minute, int second)
        {
            m_time = (unsigned __int64) (WeekDayToTicks(year, month, week, dayofweek) + TimeToTicks(hour, minute, second));
        }

        // !!! ScopeDateTime internal use only !!!
        // set m_time without any range check, would be consumed by UtcNow()
        void SetDateData(const unsigned __int64 dateData)
        {
            m_time = dateData;
        }

        __int64 Ticks() const
        {
            return GET_TICKS(m_time);
        }

        DateTimeKind Kind() const
        {
            switch (InternalKind())
            {
            case KindUnspecified:
                return Unspecified;
            case KindUtc:
                return Utc;
            default:
                return Local;
            }
        }

        ScopeDateTime AddTicks(const __int64 value) const
        {
            __int64 ticks = Ticks();
            if (value > MaxTicks - ticks || value < MinTicks - ticks)
                throw std::invalid_argument("argument out of range");

            ScopeDateTime sdt;
            sdt.SetDateData((unsigned __int64) (ticks + value) | InternalKind());
            return sdt;
        }

        ScopeDateTime AddMilliseconds(const double value) const
        {
            return Add(value, 1);
        }

        ScopeDateTime AddSeconds(const double value) const
        {
            return Add(value, MillisPerSecond);
        }

        ScopeDateTime AddMinutes(const double value) const
        {
            return Add(value, MillisPerMinute);
        }

        ScopeDateTime AddHours(const double value) const
        {
            return Add(value, MillisPerHour);
        }

        ScopeDateTime AddDays(const double value) const
        {
            return Add(value, MillisPerDay);
        }

        ScopeDateTime AddMonths(const int months) const
        {
            if (months < -120000 || months > 120000) 
                throw std::invalid_argument("argument out of range");

            __int64 ticks = Ticks();

            int y = GetDatePart(ticks, DatePartYear);
            int m = GetDatePart(ticks, DatePartMonth);
            int d = GetDatePart(ticks, DatePartDay);
            int i = m - 1 + months;
            if (i >= 0) 
            {
                m = i % 12 + 1;
                y = y + i / 12;
            }
            else 
            {
                m = 12 + (i + 1) % 12;
                y = y + (i - 11) / 12;
            }
            if (y < 1 || y > 9999) 
            {
                throw std::invalid_argument("argument out of range");
            }
            int days = DaysInMonth(y, m);
            d = d > days ? days : d;

            ScopeDateTime sdt;
            sdt.SetDateData((unsigned __int64) (DateToTicks(y, m, d)  + ticks % TicksPerDay) | InternalKind());
            return sdt;
        }

        ScopeDateTime AddYears(const int value) const
        {
            if (value < -10000 || value > 10000)
                throw std::invalid_argument("argument out of range");

            return AddMonths(value * 12);
        }

        static int DaysInMonth(const int year, const int month) 
        {
            if (month < 1 || month > 12)
                throw std::invalid_argument("month argument out of range");

            // IsLeapYear checks the year argument
            const int *days = IsLeapYear(year) ? DaysToMonth366 : DaysToMonth365;
            return days[month] - days[month - 1];
        }

        // indicate that is(was) the first occurance of the ambiguous hour
        // TODO: should be private, expose now for testing purpose
        bool IsAmbiguousDaylightSavingTime() const
        {
            unsigned __int64 flag = m_time & FlagsMask;
            return flag == KindLocalAmbiguousDst;
        }

        // align to System.DateTime.ToBinary()
        __int64 ToBinary() const
        {
            if (Kind() == Local)
            {
                // move the flag check logic only in local to avoid UTC/Unspecified perf regression
                if (ForceManagedSerialization || ForceManagedWrite)
                {
#if defined(SCOPE_RUNTIME_EXPORT_DLL) || defined(SCOPEENGINE_EXPORT_DLL)
                    SCOPE_ASSERT(!"Managed fallback for DateTime is not supported in precompiled mode");
#else
                    return ManagedScopeDateTimeToBinary(*this);
#endif
                }
                // Local times need to be adjusted as you move from one time zone to another, 
                // just as they are when serializing in text. As such the format for local times
                // changes to store the ticks of the UTC time, but with flags that look like a 
                // local date.

                // To match serialization in text we need to be able to handle cases where
                // the UTC value would be out of range. Unused parts of the ticks range are
                // used for this, so that values just past max value are stored just past the
                // end of the maximum range, and values just below minimum value are stored
                // at the end of the ticks area, just below 2^62.
                __int64 ticks = Ticks();
                __int64 storedTicks = ticks - GetLocalUtcOffsetFromLocal(this);

                if (storedTicks < 0)
                    storedTicks += TicksCeiling;

                return storedTicks | (__int64) LocalMask;
            }
            else
            {
                return m_time;
            }
        }

        // align to System.DateTime.FromBinary()
        // refer to ToBinary() for binary format, in one sentence
        // it's the original Kind with UTC ticks
        static ScopeDateTime FromBinary(__int64 binary) 
        {
            __int64 ticks = GET_TICKS(binary);

            // System.DateTime bug that even if 63~64 bit is 11, which is never a valid value ToBinary could return
            // FromBinary() will still get it parsed, e.g. DateTime.FromBinary(-3976786276152448022)
            // However we should align to System.DateTime ... 
            // So don't check if kind == KindLocal otherwise it will reject those invalid values
            unsigned __int64 kind = GET_KIND(binary);

            // Unspecified and UTC datetime, return directly
            if (kind == KindUnspecified)
            {
                if (ticks > MaxTicks || ticks < MinTicks)
                    throw std::invalid_argument("invalid binary data");
                else
                    return ScopeDateTime(ticks, Unspecified);
            }
            else if (kind == KindUtc)
            {
                if (ticks > MaxTicks || ticks < MinTicks)
                    throw std::invalid_argument("invalid binary data");
                else
                    return ScopeDateTime(ticks, Utc);
            }

            // local time, need to calculate timezone and DST offset for ticks
            else
            {
                // move the flag check logic only in local to avoid UTC/Unspecified perf regression
                if (ForceManagedSerialization || ForceManagedRead)
                {
#if defined(SCOPE_RUNTIME_EXPORT_DLL) || defined(SCOPEENGINE_EXPORT_DLL)
                    SCOPE_ASSERT(!"Managed fallback for DateTime is not supported in precompiled mode");
#else
                    return ManagedScopeDateTimeFromBinary(binary);
#endif
                }

                if (ticks > TicksCeiling - TicksPerDay)
                    ticks -= TicksCeiling;

                bool isAmbiguousLocalDst = false;
                __int64 offsetTicks;

                if (ticks < MinTicks)
                    offsetTicks = GetLocalUtcOffsetFromLocal(&MinValue);
                else if (ticks > MaxTicks)
                    offsetTicks = GetLocalUtcOffsetFromLocal(&MaxValue);
                else
                {
                    ScopeDateTime temp(ticks);
                    offsetTicks = GetLocalUtcOffsetFromUtc(&temp, isAmbiguousLocalDst);
                }

                ticks += offsetTicks;

                // wrap small times
                if (ticks < 0)
                    ticks += TicksPerDay;

                return ScopeDateTime(ticks, Local, isAmbiguousLocalDst);
            }
        }

        __int64 ToFileTime() const
        {
            return ToFileTime(false);
        }

        __int64 ToFileTime(bool alignMinTicks) const
        {
            if (Ticks() == MinTicks && alignMinTicks)
            {
                return MinTicks;
            }

            __int64 fileTime = Ticks() - FileTimeOffset;
            if(fileTime < 0)
            {
                stringstream ss;
                char buf[256];
                ToString(buf, _countof(buf));
                ss << "It's an invalid FileTime value: " << buf << ". FileTime has to be larger than 1601/1/1.";
                throw std::runtime_error(ss.str().c_str());
            }

            return fileTime;
        }

        static ScopeDateTime FromFileTime(__int64 fileTime)
        {
            return FromFileTime(fileTime, false);
        }

        static ScopeDateTime FromFileTime(__int64 fileTime, bool alignMinTicks)
        {
            if (fileTime ==  MinTicks && alignMinTicks)
            {
                return ScopeDateTime::MinValue;
            }

            return ScopeDateTime(fileTime + FileTimeOffset);
       }

        int ToString(char *outBuf, int bufSize) const
        {
            int result = -1;

            if (ForceManagedSerialization || ForceManagedWrite)
            {
#if defined(SCOPE_RUNTIME_EXPORT_DLL) || defined(SCOPEENGINE_EXPORT_DLL)
                SCOPE_ASSERT(!"Managed fallback for DateTime is not supported in precompiled mode");
#else
                return ManagedScopeDateTimeToString(*this, outBuf, bufSize, NULL);
#endif
            }

            result = ToString(outBuf, bufSize, NULL, CurrentDateTimeFormatInfo);
            if (result == -1 && AllowManagedFallback)
            {
#if defined(SCOPE_RUNTIME_EXPORT_DLL) || defined(SCOPEENGINE_EXPORT_DLL)
                SCOPE_ASSERT(!"Managed fallback for DateTime is not supported in precompiled mode");
#else
                return ManagedScopeDateTimeToString(*this, outBuf, bufSize, NULL);
#endif
            }

            return result;
        }

        int ToString(char *outBuf, int bufSize, const char *format) const
        {
            int result = -1;

            if (ForceManagedSerialization || ForceManagedWrite)
            {
#if defined(SCOPE_RUNTIME_EXPORT_DLL) || defined(SCOPEENGINE_EXPORT_DLL)
                SCOPE_ASSERT(!"Managed fallback for DateTime is not supported in precompiled mode");
#else
                return ManagedScopeDateTimeToString(*this, outBuf, bufSize, format);
#endif
            }

            result = ToString(outBuf, bufSize, format, CurrentDateTimeFormatInfo);
            if (result == -1 && AllowManagedFallback)
            {
#if defined(SCOPE_RUNTIME_EXPORT_DLL) || defined(SCOPEENGINE_EXPORT_DLL)
                SCOPE_ASSERT(!"Managed fallback for DateTime is not supported in precompiled mode");
#else
                return ManagedScopeDateTimeToString(*this, outBuf, bufSize, format);
#endif
            }

            return result;
        }

        // Limited alignment to CLR DateTime.ToString(), support en-us only
        // Return -1 for non support locale, in that case caller should fall back to CLR DateTime.ToString()
        int ToString(char *outBuf, int bufSize, const char *format, const ScopeDateTimeFormatInfo &sdtfi) const
        {
            if (sdtfi.FallBackToClrHelper)
            {
                return -1;
            }

            char realFormat[MaxToStringLen];
            int realFormatSize = 0;

            // default is "G" -- general long time pattern
            strcpy_s(realFormat, IsNullOrEmpty(format) ? "G" : format);

            if (strlen(realFormat) == 1)
            {
                if(*realFormat == 'U' && Kind() != Utc)
                {
                    // For "U" format, we change it to UTC first
                    __int64 offset = GetLocalUtcOffsetFromLocal(this);
                    ScopeDateTime sdt = ScopeDateTime(Ticks() - offset, Utc);
                    realFormatSize = ExpandPredefinedFormat(realFormat, MaxToStringLen, sdtfi);
                    return sdt.ToString(outBuf, bufSize, realFormat);
                }

                realFormatSize = ExpandPredefinedFormat(realFormat, MaxToStringLen, sdtfi);                
            }
            else
            {
                realFormatSize = (int) strlen(format);
            }

            return FormatCustomized(outBuf, bufSize, realFormat, realFormatSize, sdtfi);
        }

        // Only support the output of ScopeDateTime.ToString() fow now, which is "G" format,  "M/d/yyyy h:mm:ss(.fff) tt"
        // Return true if matching format and convert to ScopeDateTime successfully
        // Otherwise false if not support yet or bad time string, caller should fall back to CLR then
        static bool TryParse(const char *str, ScopeDateTime &sdt)
        {
            if (ForceManagedSerialization || ForceManagedRead)
            {
                return  ManagedTryParseScopeDateTime(str, sdt);
            }
			
            bool result = TryParse(str, CurrentDateTimeFormatInfo, sdt);
            if ( !result && AllowManagedFallback)
            {
                return ManagedTryParseScopeDateTime(str, sdt);
            }

            return result;
        }

        static ScopeDateTime Parse(const char *str)
        {
            ScopeDateTime sdt;

            if (!TryParse(str, sdt))
            {
                throw std::runtime_error("Bad DateTime string");
            }

            return sdt;
        }

        // current implement takes assumption that input string (c style const char*) is null terminiated
        // however for FString and casted DTStringInput, it's not the case so we might face buffer overflow, TFS #748158 opened to track
        static ScopeDateTime Parse(const DTStringInput &input)
        {
            return Parse(input.str.c_str());
        }

        SCOPE_ENGINE_API static bool TryParse(const char *str, const ScopeDateTimeFormatInfo &sdtfi, ScopeDateTime &sdt);

		ScopeDateTime Date() const
		{
			__int64 ticks = Ticks() / TicksPerDay * TicksPerDay;	
			ScopeDateTime res;
			res.SetDateData(ticks | InternalKind());
			return res;
		}

		static ScopeDateTime Today()
		{
			return Now().Date();
		}


        int Year() const
        {
            return GetDatePart(Ticks(), DatePartYear);
        }

        int Month() const
        {
            return GetDatePart(Ticks(), DatePartMonth);
        }

        int Day() const
        {
            return GetDatePart(Ticks(), DatePartDay);
        }

        int DayOfWeek() const
        {
            return GetDatePart(Ticks(), DatePartDayOfWeek);
        }

        int Hour() const
        {
            return (int) ((Ticks() / TicksPerHour) % 24);
        }

        int Minute() const
        {
            return (int) ((Ticks() / TicksPerMinute) % 60); 
        }

        int Second() const
        {
            return (int) ((Ticks() / TicksPerSecond) % 60); 
        }

        int Millisecond() const
        {
            return (int) ((Ticks() / TicksPerMillisecond) % 1000);
        }

        static bool IsLeapYear(int year)
        {
            if (year < MinYear || year > MaxYear)
                throw std::invalid_argument("not supported year");

            return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        }

        SCOPE_ENGINE_API static ScopeDateTime UtcNow();

        static ScopeDateTime Now()
        {
            ScopeDateTime utc = UtcNow();
            bool isAmbiguousLocalDst = false;
            __int64 offset = GetLocalUtcOffsetFromUtc(&utc, isAmbiguousLocalDst);
            __int64 ticks = utc.Ticks() + offset;
            
            // align to CLR, throttle out of range values
            if (ticks > MaxTicks)
                return ScopeDateTime(MaxTicks, Local);
            if (ticks < MinTicks)
                return ScopeDateTime(MinTicks, Local);

            return ScopeDateTime(ticks, Local, isAmbiguousLocalDst);
        }

        ScopeDateTime & operator= ( ScopeDateTime const& rhs )
        {
            this->m_time = rhs.m_time;
            return *this;
        }

        // Note: The comparison operators have slightly non-intuative semantics
        // they completely ignore the timezone the time is in
        // It is the callers responsibility to make sure that the timezone matches
        bool operator < (const ScopeDateTime & t ) const
        {
            return GET_TICKS(m_time) < GET_TICKS(t.m_time);
        }

        bool operator == (const ScopeDateTime & t ) const
        {
            return GET_TICKS(m_time) == GET_TICKS(t.m_time);
        }

        bool operator != (const ScopeDateTime & t ) const
        {
            return GET_TICKS(m_time) != GET_TICKS(t.m_time);
        }

        bool operator > (const ScopeDateTime & t ) const
        {
            return GET_TICKS(m_time) > GET_TICKS(t.m_time);
        }

        bool operator <= (const ScopeDateTime & t ) const
        {
            return GET_TICKS(m_time) <= GET_TICKS(t.m_time);
        }

        bool operator >= (const ScopeDateTime & t ) const
        {
            return GET_TICKS(m_time) >= GET_TICKS(t.m_time);
        }

        // compute 32 bit hash for ScopeDateTime
        // This must match the .NET code
        //
        // public override int GetHashCode() {
        //    Int64 ticks = InternalTicks;
        //    return unchecked((int)ticks) ^ (int)(ticks >> 32);
        //}
        int GetScopeHashCode() const
        {
            __int64 internalTicks = GET_TICKS(m_time);
            return (((int) internalTicks) ^ ((int) (internalTicks >> 0x20)));
        }

        unsigned __int64 GetCRC32Hash(unsigned __int64 crc) const
        {
            return _mm_crc32_u64(crc, GET_TICKS(m_time));
        }

	private:
        enum DatePart
        {
            DatePartYear = 0,
            DatePartDayOfYear = 1,
            DatePartMonth = 2,
            DatePartDay = 3,
            DatePartDayOfWeek = 4
        };

        // most copied from System.DateTime
        static int GetDatePart(const __int64 ticks, const DatePart part)
        {
            // n = number of days since 1/1/0001
            int n = (int)(ticks / TicksPerDay);
            
            if(part == DatePartDayOfWeek)
            {
                return (n+1) % 7;
            }

            // y400 = number of whole 400-year periods since 1/1/0001
            int y400 = n / DaysPer400Years;
            // n = day number within 400-year period
            n -= y400 * DaysPer400Years;
            // y100 = number of whole 100-year periods within 400-year period
            int y100 = n / DaysPer100Years;
            // Last 100-year period has an extra day, so decrement result if 4
            if (y100 == 4) y100 = 3;
            // n = day number within 100-year period
            n -= y100 * DaysPer100Years;
            // y4 = number of whole 4-year periods within 100-year period
            int y4 = n / DaysPer4Years;
            // n = day number within 4-year period
            n -= y4 * DaysPer4Years;
            // y1 = number of whole years within 4-year period
            int y1 = n / DaysPerYear;
            // Last year has an extra day, so decrement result if 4
            if (y1 == 4) y1 = 3;
            // If year was requested, compute and return it
            if (part == DatePartYear) {
                return y400 * 400 + y100 * 100 + y4 * 4 + y1 + 1;
            }
            // n = day number within year
            n -= y1 * DaysPerYear;
            // If day-of-year was requested, return it
            if (part == DatePartDayOfYear) return n + 1;
            // Leap year calculation looks different from IsLeapYear since y1, y4,
            // and y100 are relative to year 1, not year 0
            bool leapYear = y1 == 3 && (y4 != 24 || y100 == 3);
            const int *days = leapYear ? DaysToMonth366: DaysToMonth365;
            // All months have less than 32 days, so n >> 5 is a good conservative
            // estimate for the month
            int m = (n >> 5) + 1;
            // m = 1-based month number
            while (n >= days[m]) m++;
            // If month was requested, return it
            if (part == DatePartMonth) return m;
            // Return 1-based day-of-month
            return n - days[m - 1] + 1;
        }

        // most copied from System.DateTime
        static __int64 DateToTicks(int year, int month, int day)
        {     
            if (year >= MinYear && year <= MaxYear && month >= 1 && month <= 12) 
            {
                const int *days = IsLeapYear(year) ? DaysToMonth366: DaysToMonth365;
                if (day >= 1 && day <= days[month] - days[month - 1]) {
                    int y = year - 1;
                    int n = y * 365 + y / 4 - y / 100 + y / 400 + days[month - 1] + day - 1;
                    return n * TicksPerDay;
                }
                else
                    throw std::invalid_argument("argument out of range");
            }
            else
                throw std::invalid_argument("argument out of range");
        }

        /*
        * for convenience of calculating DST dates, which is particular occurance of certain weekday
        * week -- the occurance time (5 means last occurance)
        * dayofweek - 0 == Sunday
        */
        static __int64 WeekDayToTicks(int year, int month, int week, int dayofweek)
        {
            const int *days = IsLeapYear(year) ? DaysToMonth366: DaysToMonth365;

            // figure the day of the week of the month start
            int y = year - 1;
            int monthdow = (y * 365 + y / 4 - y / 100 + y / 400 + days[month - 1] + 1) % 7;
            // figure the exact date of month
            int date = monthdow <= dayofweek ? ((dayofweek - monthdow) + (week-1) * 7 + 1) : ((dayofweek - monthdow) + week * 7 + 1);

            // out of month range
            if ( week == 5 && date > (days[month] - days[month-1]))
                date -= 7;

            return DateToTicks(year, month, date);
        }

        static __int64 TimeToTicks(int hour, int minute, int second)
        {
            if (hour >= 0 && hour < 24 && minute >= 0 && minute < 60 && second >=0 && second < 60)
            {
                return (hour * TicksPerHour) + (minute * TicksPerMinute) + (second * TicksPerSecond); 
            }
            else
            {
                throw std::invalid_argument("argument out of range");
            }
        }

        /*
        * return (Local - UTC) offset, in ticks, from local perspective
        * scopeDT -- the **Local** time point with **LOCAL or Unspecified** kind
        */
        SCOPE_ENGINE_API static __int64 GetLocalUtcOffsetFromLocal(const ScopeDateTime *scopeDT);

        /*
        * return (Local - UTC) offset, in ticks, from UTC perspective
        * scopeDT -- the **UTC** time point with **UTC/Unspecified** kind
        */
        SCOPE_ENGINE_API static __int64 GetLocalUtcOffsetFromUtc(const ScopeDateTime *scopeDT, bool &isAmbiguousLocalDst);

        // expand predefined standard datetime format
        // return value is the expanded string length
        static int ExpandPredefinedFormat(char *format, int formatSize, const ScopeDateTimeFormatInfo &sdtfi)
        {
            char ch = format[0];
            switch (ch){
            case 'G':
                strcpy_s(format, formatSize, sdtfi.GeneralLongTimePattern.c_str());
                return (int) sdtfi.GeneralLongTimePattern.length();
                break;
            case 'g':
                strcpy_s(format, formatSize, sdtfi.GeneralShortTimePattern.c_str());
                return (int) sdtfi.GeneralShortTimePattern.length();
                break;
            case 'F':
                strcpy_s(format, formatSize, sdtfi.FullLongTimePattern.c_str());
                return (int) sdtfi.FullLongTimePattern.length();
                break;
            case 'f':
                strcpy_s(format, formatSize, sdtfi.FullShortTimePattern.c_str());
                return (int) sdtfi.FullShortTimePattern.length();
                break;
            case 'D':
                strcpy_s(format, formatSize, sdtfi.LongDatePattern.c_str());
                return (int) sdtfi.LongDatePattern.length();
                break;
            case 'd':
                strcpy_s(format, formatSize, sdtfi.ShortDatePattern.c_str());
                return (int) sdtfi.ShortDatePattern.length();
                break;
            case 'O': 
            case 'o':
                strcpy_s(format, formatSize, sdtfi.RoundtripFormat.c_str());
                return (int) sdtfi.RoundtripFormat.length();
                break;
            case 'T':
                strcpy_s(format, formatSize, sdtfi.LongTimePattern.c_str());
                return (int) sdtfi.LongTimePattern.length();
                break;
            case 't':
                strcpy_s(format, formatSize, sdtfi.ShortTimePattern.c_str());
                return (int) sdtfi.ShortTimePattern.length();
                break;
            case 'r': 
            case 'R':
                strcpy_s(format, formatSize, sdtfi.RFC1123DatePattern.c_str());
                return (int) sdtfi.RFC1123DatePattern.length();
                break;
            case 's':
                strcpy_s(format, formatSize, sdtfi.SortableFormatPattern.c_str());
                return (int) sdtfi.SortableFormatPattern.length();
                break;
            case 'U':
                strcpy_s(format, formatSize, sdtfi.UniversalFullDatePattern.c_str());
                return (int) sdtfi.UniversalFullDatePattern.length();
                break;
            case 'u':
                strcpy_s(format, formatSize, sdtfi.UniversalSortableFormatPattern.c_str());
                return (int) sdtfi.UniversalSortableFormatPattern.length();
                break;
            case 'M': 
            case 'm':         
                strcpy_s(format, formatSize, sdtfi.MonthDayPattern.c_str());
                return (int) sdtfi.MonthDayPattern.length();
                break;
            case 'Y': 
            case 'y':         
                strcpy_s(format, formatSize, sdtfi.YearMonthPattern.c_str());
                return (int) sdtfi.YearMonthPattern.length();
                break;        
            default:
                throw std::invalid_argument("bad datetime format string");
            }
        }

        // converted from system.globalization.datetimeformat
        int FormatCustomized(char *outBuf, int size, const char *format, int formatLen, const ScopeDateTimeFormatInfo &sdtfi) const
        {
            // direct return if buffer size not greater than zero
            if (size <= 0)
                return 0;

            char *outIndex = outBuf;

            // This is a flag to indicate if we are formating hour/minute/second only.
            bool bTimeOnly = true;

            int tokenLen, hour, hour12;
            int year, month;

            for (int i=0; i < formatLen && (outIndex - outBuf) < size; i += tokenLen) 
            {
                char ch = format[i];
                char nextChar;
                switch (ch) 
                {
                case 'h':
                    tokenLen = ParseRepeatPattern(format, formatLen, i, ch);
                    hour12 = Hour() % 12;
                    if (hour12 == 0)
                    {
                        hour12 = 12;
                    }
                    outIndex += FormatDigits(outIndex, size - (int) (outIndex - outBuf), hour12, tokenLen);
                    break;
                case 'H':
                    tokenLen = ParseRepeatPattern(format, formatLen, i, ch);
                    outIndex += FormatDigits(outIndex, size - (int) (outIndex - outBuf), Hour(), tokenLen);
                    break;
                case 'm':
                    tokenLen = ParseRepeatPattern(format, formatLen, i, ch);
                    outIndex += FormatDigits(outIndex, size - (int) (outIndex - outBuf), Minute(), tokenLen);
                    break;
                case 's':
                    tokenLen = ParseRepeatPattern(format, formatLen, i, ch);
                    outIndex += FormatDigits(outIndex, size - (int) (outIndex - outBuf), Second(), tokenLen);
                    break;
                case 't':
                    hour = Hour();
                    tokenLen = ParseRepeatPattern(format, formatLen, i, ch);
                    if (tokenLen == 1)
                    {
                        if (hour < 12)
                        {
                            if (sdtfi.AmDesignator.length() >= 1)
                            {
                                *(outIndex++) = sdtfi.AmDesignator.at(0);
                            }
                        }
                        else
                        {
                            if (sdtfi.PmDesignator.length() >= 1)
                            {
                                *(outIndex++) = sdtfi.PmDesignator.at(0);
                            }
                        }
                    }
                    else
                    {
                        if (hour < 12)
                        {
                            strcpy_s(outIndex, size - (outIndex - outBuf), sdtfi.AmDesignator.c_str());
                            outIndex += (int) sdtfi.AmDesignator.length();
                        }
                        else
                        {
                            strcpy_s(outIndex, size - (outIndex - outBuf), sdtfi.PmDesignator.c_str());
                            outIndex += (int) sdtfi.PmDesignator.length();
                        }
                    }
                    break;
                case 'd':
                    //
                    // tokenLen == 1 : Day of month as digits with no leading zero.
                    // tokenLen == 2 : Day of month as digits with leading zero for single-digit months.
                    // tokenLen == 3 : Day of week as a three-leter abbreviation.
                    // tokenLen >= 4 : Day of week as its full name.
                    //
                    tokenLen = ParseRepeatPattern(format, formatLen, i, ch);
                    if (tokenLen <= 2)
                        outIndex += FormatDigits(outIndex, size - (int) (outIndex - outBuf), Day(), tokenLen);
                    else
                        outIndex += FormatDayOfWeek(outIndex, size - (int) (outIndex - outBuf), DayOfWeek(), tokenLen, sdtfi);
                    bTimeOnly = false;
                    break;
                case 'F':
                case 'f':
                    tokenLen = ParseRepeatPattern(format, formatLen, i, ch);
                    if (tokenLen <= MaxSecondsFractionDigits)
                    {
                        int fraction = (int) (Ticks() % TicksPerSecond); // int is enough for 10000 * 1000
                        fraction /= (int) pow(10.0, MaxSecondsFractionDigits - tokenLen);
                        if(ch == 'f')
                        {
                            outIndex += FormatDigits(outIndex, size - (int) (outIndex - outBuf), fraction, tokenLen, true);
                        }
                        else 
                        {
                            int effectiveDigits = tokenLen;
                            while (effectiveDigits > 0 && fraction % 10 == 0) 
                            {
                                fraction /= 10;
                                effectiveDigits--;
                            }
                            if (effectiveDigits > 0) 
                            {
                                outIndex += FormatDigits(outIndex, size - (int) (outIndex - outBuf), fraction, effectiveDigits, true);
                            }
                            else if(outIndex != outBuf && *(outIndex - 1) == '.')
                            {
                                outIndex--;  // No fraction to emit, so we should remove dot as well
                            }
                        }
                    }
                    else
                    {
                        throw std::invalid_argument("bad datetime format string");
                    }
                    break;
                case 'M':
                    // 
                    // tokenLen == 1 : Month as digits with no leading zero.
                    // tokenLen == 2 : Month as digits with leading zero for single-digit months.
                    // tokenLen == 3 : Month as a three-letter abbreviation.
                    // tokenLen >= 4 : Month as its full name.
                    //
                    tokenLen = ParseRepeatPattern(format, formatLen, i, ch);
                    month = Month();
                    if (tokenLen <= 2)
                        outIndex += FormatDigits(outIndex, size - (int) (outIndex - outBuf), month, tokenLen);
                    else
                        outIndex += FormatMonth(outIndex, size - (int) (outIndex - outBuf), month, tokenLen, sdtfi);
                    bTimeOnly = false;
                    break;
                case 'y':
                    // Notes about OS behavior:
                    // y: Always print (year % 100). No leading zero.
                    // yy: Always print (year % 100) with leading zero.
                    // yyy/yyyy/yyyyy/... : Print year value.  No leading zero.
                    year = Year();
                    tokenLen = ParseRepeatPattern(format, formatLen, i, ch);
                    if (tokenLen <= 2) 
                        outIndex += FormatDigits(outIndex, size - (int) (outIndex - outBuf), year % 100, tokenLen);
                    else
                        outIndex += FormatDigits(outIndex, size - (int) (outIndex - outBuf), year, tokenLen, true);
                    bTimeOnly = false;
                    break;
                case ':':
                    strcpy_s(outIndex, size - (outIndex - outBuf), sdtfi.TimeSeparator.c_str());
                    tokenLen = 1;
                    outIndex += (int) sdtfi.TimeSeparator.length();
                    break;
                case '/':
                    strcpy_s(outIndex, size - (outIndex - outBuf), sdtfi.DateSeparator.c_str());
                    tokenLen = 1;
                    outIndex += (int) sdtfi.DateSeparator.length();
                    break;
                case '\'':
                case '\"':
                    tokenLen = ParseQuoteString(outIndex, size - (int) (outIndex - outBuf), format, formatLen, i);
                    outIndex += (tokenLen -2);
                    break;
                case '\\':
                    nextChar = ParseNextChar(format , formatLen, i);
                    if (nextChar >= 0)
                    {
                        *(outIndex++) = nextChar;
                        tokenLen = 2;
                    } 
                    else
                    {
                        throw std::invalid_argument("bad datetime format string");
                    }
                    break;                
                case '%':
                    nextChar = ParseNextChar(format, formatLen, i);
                    if (nextChar >= 0 && nextChar != '%') 
                    {
                        outIndex += FormatCustomized(outIndex, size - (int) (outIndex - outBuf), &nextChar, 1, sdtfi);
                        tokenLen = 2;
                    }
                    else
                    {
                        throw std::invalid_argument("bad datetime format string");
                    }
                    break;
                case 'K':
                    tokenLen = 1;
                    outIndex += FormatCustomizedRoundripTimeZone(outIndex, size - (int) (outIndex - outBuf));
                    break;
                case 'g':
                    tokenLen = ParseRepeatPattern(format, formatLen, i, ch);
                    strcpy_s(outIndex, size - (outIndex - outBuf), sdtfi.EraString.c_str());
                    break;
                case 'z':
                    tokenLen = ParseRepeatPattern(format, formatLen, i, ch);
                    outIndex += FormatCustomizedTimeZone(outIndex, size - (int) (outIndex - outBuf), tokenLen, bTimeOnly);
                    break;
                    
                default:
                    // NOTENOTE : we can remove this rule if we enforce the enforced quote
                    // character rule.
                    // That is, if we ask everyone to use single quote or double quote to insert characters,
                    // then we can remove this default block.
                    *(outIndex++) = ch;
                    tokenLen = 1;
                    break;
                }
            }

            int resultStrLen = (int) (outIndex - outBuf);

            // check if buffer is full
            if (resultStrLen > size - 1)
            {
                *outBuf = '\0';
                return 0;
            }

            *outIndex = '\0';
            return resultStrLen;
        }

        // MonthNames[0] for 1st month, MonthNames[1] for 2nd month, and so on.
        int FormatMonth(char *out, int outSize, int value, int tokenLen, const ScopeDateTimeFormatInfo &sdtfi) const
        {    
            string monthString = tokenLen == 3 ? sdtfi.AbbreviatedMonthNames[value - 1] : sdtfi.MonthNames[value - 1];
            int len = (int) monthString.length();
            strncpy_s(out, outSize, monthString.c_str(), len);
            return len;
        }

        // DayOfWeekName[0] for 1st month, DayOfWeekName[1] for 2nd month, and so on.
        int FormatDayOfWeek(char *out, int outSize, int value, int tokenLen, const ScopeDateTimeFormatInfo &sdtfi) const
        {
            string dayString = tokenLen == 3 ? sdtfi.AbbreviatedDayOfWeekName[value - 1].c_str() : sdtfi.DayOfWeekName[value - 1].c_str();
            int len = (int) dayString.length();
            strncpy_s(out, outSize, dayString.c_str(), len);
            return len;
        }

        // aligns to FormatCustomizedTimeZone in DateFimeFormat
        int FormatCustomizedTimeZone(char* out, int outSize, int tokenLen, bool timeOnly) const
        {
            // minimal output length would be 2 (e.g. -7)
            if (outSize < 2)
                throw std::invalid_argument("not enough buffer size");

            __int64 offset = 0;

            // get offset, and we strictly aligns to FormatCustomizedTimeZone in DateFimeFormat in CLR
            if (timeOnly && Ticks() < TicksPerDay)
            {
                // For time only format and a time only input, the time offset on 0001/01/01 is less 
                // accurate than the system's current offset because of daylight saving time.
                ScopeDateTime now = Now();
                offset = GetLocalUtcOffsetFromLocal(&now);
            }
            else if(Kind() == Utc)
            {
#ifdef SDT_TEST
                // to pass MDA related CLR test cases
                MDA_InvalidFormatForUtc = true;
#endif
                ScopeDateTime asLocal = ScopeDateTime(Ticks(), Local);
                offset = GetLocalUtcOffsetFromLocal(&asLocal);
            }
            else
            {
                offset = GetLocalUtcOffsetFromLocal(this);
            }

            return FormatTimezoneOffset(out, outSize, offset, tokenLen);
        }

        // converted from system.globalization.datetimeformat
        int FormatCustomizedRoundripTimeZone(char* out, int outSize) const
        {
            __int64 offset = 0;
            DateTimeKind kind = Kind();

            if (kind == Unspecified)
            {
                return 0;
            }
            else if (outSize < 1)
            {
                throw std::invalid_argument("not enough buffer size");
            }
            else if (kind == Utc)
            {
                // The 'Z' constant is a marker for a UTC date
                *out = 'Z';
                return 1;
            }
            else
            {
                // Local kind, fall through to shared time zone output code
                offset = GetLocalUtcOffsetFromLocal(this);
                return FormatTimezoneOffset(out, outSize, offset, 3);
            }
        }

        // tokenLen == 1 : offset hour as digits with no leading zero.  (-7 -> -7, 11 -> +11)
        // tokenLen == 2 : offset hour as digits with leading zero for single-digit hour. (-7 -> -07, 11 -> +11)
        // tokenLen >= 3 : offset hours and minutes as digits with leading zero for single-digit hour/minute (-7 -> -07:00, 11 -> +11:00)
        int FormatTimezoneOffset(char *out, int outSize, __int64 offset, int tokenLen) const
        {
            offset /= TicksPerMinute;

            if (offset >= 0) 
            {
                *out = '+';
            }
            else 
            {
                *out = '-';
                offset = -offset; // range of offset for timezone is safe to return negative value
            }

            int hours = (int) offset / 60;
            int minutes = (int) offset % 60;

            if (tokenLen <= 2)
            {
                return FormatDigits(out + 1, outSize - 1, hours, tokenLen) + 1;
            }
            else 
            {
                FormatDigits(out + 1, outSize - 1, hours, tokenLen);
                FormatDigits(out + 4, outSize - 4, minutes, tokenLen);
                *(out + 3) = ':';
                return 6;  // length of (+/-)XX:XX
            } 
        }

        // converted from system.globalization.datetimeformat
        char ParseNextChar(const char* format, int formatLen, int pos) const
        {
            return (pos >= formatLen - 1) ? -1 : format[pos+1];
        }

        // converted from system.globalization.datetimeformat
        int ParseQuoteString(char* out, int outSize, const char* format, int formatLen, int pos) const
        {
            char *p = out;

            int beginPos = pos;
            char quoteChar = format[pos++]; // Get the character used to quote the following string.

            bool foundQuote = false;
            while (pos < formatLen)
            {
                char ch = format[pos++];        
                if (ch == quoteChar)
                {
                    foundQuote = true;
                    break;
                }
                else if (ch == '\\') 
                {
                    if (pos < formatLen)
                    {
                        *(p++) = format[pos++];
                        outSize--;
                    }
                    else
                    {
                        throw std::invalid_argument("bad datetime format string");
                    }
                } 
                else 
                {
                    *(p++) = ch;
                    outSize--;
                }

                if (outSize == 0)
                    throw std::invalid_argument("not enough buffer size");
            }

            if (!foundQuote)
            {
                throw std::invalid_argument("Format_BadQuote");
            }

            return (pos - beginPos);
        }

        // converted from system.globalization.datetimeformat
        int ParseRepeatPattern(const char *format, int formatLen, int pos, char patternChar) const
        {
            int index;
            for (index = pos + 1;(index < formatLen) && (format[index]== patternChar); ++index)
            {
            }

            return (index - pos);
        }

        // converted from system.globalization.datetimeformat
        int FormatDigits(char *out, int outSize, int value, int len) const
        {
            return FormatDigits(out, outSize, value, len, false);
        }

        // converted from system.globalization.datetimeformat
        // convert "value" into "out", adding leading zeros if necessary
        // if overrideLengthLimit is false, the formatted integer length is throttled as 2, regardless of "len"
        // this is used by formatting hour ("hhhhhhh" -> "hh"), minutes, and etc.
        // if overrideLengthLimit is true, the formatted integer length is "len"
        // this is used by formatting year ("yyy", "yyyy", "yyyyy" ... )
        int FormatDigits(char *out, int outSize, int value, int len, bool overrideLengthLimit) const
        {
            len = (!overrideLengthLimit && len > 2) ? 2 : len;

            char buffer[16];
            char *p = buffer + 16;
            int n = value;
            do {
                *--p = (char)(n % 10 + '0');
                n /= 10;
            } while ((n != 0)&&(p > buffer));

            int digits = (int) (buffer + 16 - p);

            //If the repeat count is greater than 0, we're trying
            //to emulate the "00" format, so we have to prepend
            //a zero if the string only has one character.
            while ((digits < len) && (p > buffer)) 
            {
                *--p='0';
                digits++;
            }
            strncpy_s(out, outSize, p, digits);
            return digits;
        }

        // skipping all white spaces and advance the index
        static void SkipWhiteSpaces(const char * &index)
        {
            while ( isspace(*index) )
            {
                ++index;
            }
        }

        // get number and advance the index
        // return the length of the number (include leading 0, if any)
        // return -1 when not a number or exceed max length
        static int GetNumber(const char * &index, int &value)
        {
            value = -1;
            int digitCount = 0;

            while ( isdigit(*index) && digitCount++ < MaxDateTimeNumberDigits)
            {
                value = (value == -1 ? 0 : value) * 10 + (*index++ - '0');
            }

            if (digitCount == 0 || digitCount > MaxDateTimeNumberDigits)
            {
                return -1;
            }
            else
            {
                return digitCount;
            }
        }

        // check if it's desired separator, and advance index if true
        static bool CheckSeparator(const char * &index, const string &separator)
        {
            if (StartsWith(index, separator))
            {
                index += separator.length();
                return true;
            }
            else
            {
                return false;
            }
        }

        static bool IsNullOrEmpty(const char *str)
        {
            return (str == NULL || *str == '\0');
        }

        static bool StartsWith(const char *str1, const string &str2)
        {
            return strncmp(str1, str2.c_str(), str2.length()) == 0;
        }
		
		static ScopeDateTime SpecifyKind(ScopeDateTime parsedate, DateTimeKind kind)
		{
			return ScopeDateTime(parsedate.Ticks(), kind);
		}
    };

	// TimeSpan represents a duration of time.  A TimeSpan can be negative or positive.
	//
    // TimeSpan is internally represented as a number of milliseconds.  While
    // this maps well into units of time such as hours and days, any
    // periods longer than that aren't representable in a nice fashion.
    // For instance, a month can be between 28 and 31 days, while a year
    // can contain 365 or 364 days.  A decade can have between 1 and 3 leapyears,
    // depending on when you map the TimeSpan into the calendar.  This is why
    // we do not provide Years() or Months().
	class ScopeTimeSpan
	{
		__int64 _ticks;

        friend class ScopeDateTimeParse;
		friend struct DateTimeOffset;

	public:
   		static const __int64 TicksPerMillisecond =  10000; 
		static const __int64 TicksPerSecond = TicksPerMillisecond * 1000;	// 10,000,000
		static const __int64 TicksPerMinute = TicksPerSecond * 60;			// 600,000,000
		static const __int64 TicksPerHour = TicksPerMinute * 60;			// 36,000,000,000
		static const __int64 TicksPerDay = TicksPerHour * 24;				// 864,000,000,000
        static const __int64 TicksPerWeek = TicksPerDay * 7;

		static const int MillisPerSecond = 1000;
		static const int MillisPerMinute = MillisPerSecond * 60; //     60,000
		static const int MillisPerHour = MillisPerMinute * 60;   //  3,600,000
		static const int MillisPerDay = MillisPerHour * 24;      // 86,400,000
		 
		static const __int64 ZeroTicks = 0;
		static const __int64 MaxTicks = 0x7FFFFFFFFFFFFFFF;
		static const __int64 MinTicks = 0x8000000000000000;

		static const __int64 MaxSeconds = MaxTicks / TicksPerSecond;
		static const __int64 MinSeconds = MinTicks / TicksPerSecond;
		static const __int64 MaxMilliSeconds = MaxTicks / TicksPerMillisecond;
		static const __int64 MinMilliSeconds = MinTicks / TicksPerMillisecond;

		static const __int64 TicksPerTenthSecond = TicksPerMillisecond * 100;

		ScopeTimeSpan():_ticks(0)
		{
		}
		
		ScopeTimeSpan(const ScopeTimeSpan &c):_ticks(c._ticks)
		{
		}
		
		explicit ScopeTimeSpan(const __int64 ticks):_ticks(ticks)
		{
		}
		
		ScopeTimeSpan(int hours, int minutes, int seconds):_ticks(TimeToTicks(hours, minutes, seconds))
		{
		}
		
		ScopeTimeSpan(int days, int hours, int minutes, int seconds, int milliseconds = 0)
		{
            __int64 totalMilliSeconds = ((__int64)days * 3600 * 24 + (__int64)hours * 3600 + 
				(__int64)minutes * 60 + seconds) * MillisPerSecond + milliseconds;
            if (totalMilliSeconds > MaxMilliSeconds || totalMilliSeconds < MinMilliSeconds)
				throw std::invalid_argument("milliseconds out of supporting range ");
            _ticks =  (__int64)totalMilliSeconds * TicksPerMillisecond;
		}
		
		__int64 Ticks() const
		{
			return _ticks;
		}
		
		int Days() const 
		{
			return (int)(_ticks / TicksPerDay);
		}
		
		int Hours() const 
		{
			return (int)(_ticks / TicksPerHour % 24);
		}
        
		int Milliseconds() const 
		{
			return (int)(_ticks / TicksPerMillisecond % 1000);
        }
        
		int Minutes() const
		{
			return (int)(_ticks / TicksPerMinute % 60);
        }
        
		int Seconds() const
		{
			return (int)((_ticks / TicksPerSecond) % 60);
        }
		
		double TotalDays() const 
		{
            return (double)_ticks / TicksPerDay;
        }
		
		double TotalHours() const
		{
            return (double)_ticks / TicksPerHour;
        }
		
		double TotalMilliseconds() const
		{
            double temp = (double)_ticks / TicksPerMillisecond;
            if (temp > MaxMilliSeconds)
                return (double)MaxMilliSeconds;
            if (temp < MinMilliSeconds)
                return (double)MinMilliSeconds;
            return temp;
        }
		
		double TotalMinutes() const
		{
            return (double)_ticks / TicksPerMinute;
        }
		
		double TotalSeconds() const
		{
            return (double)_ticks / TicksPerSecond;
        }

		ScopeTimeSpan Add(const ScopeTimeSpan& ts) const
		{
            __int64 result = _ticks + ts._ticks;
            // Overflow if signs of operands was identical and result's
            // sign was opposite.
            // >> 63 gives the sign bit (either 64 1's or 64 0's).
            if (((_ticks & 0x8000000000000000) == (ts._ticks & 0x8000000000000000)) && 
				((_ticks & 0x8000000000000000) != (result & 0x8000000000000000)))
                throw std::invalid_argument("argument out of supporting range");
            return ScopeTimeSpan(result);
        }
		
        // Compares two ScopeTimeSpan values, returning an integer that indicates their
        // relationship.
        static int Compare(const ScopeTimeSpan& t1, const ScopeTimeSpan& t2)
		{
            if (t1._ticks > t2._ticks) return 1;
            if (t1._ticks < t2._ticks) return -1;
            return 0;
        }

        int CompareTo(const ScopeTimeSpan& value) const
		{
			return Compare(*this, value);
        }

        static ScopeTimeSpan FromDays(double value) 
		{
            return Interval(value, MillisPerDay);
        }

        ScopeTimeSpan Duration() const
		{
            if (Ticks()==MinTicks)
                throw std::invalid_argument("overflow_duration");
            return ScopeTimeSpan(_ticks >= 0? _ticks: -_ticks);
        }

        bool Equals(const ScopeTimeSpan& obj) const
        {
			return Equals(*this, obj);
        }
        
		static bool Equals(const ScopeTimeSpan& t1, const ScopeTimeSpan& t2)
		{
            return t1._ticks == t2._ticks;
        }
        
		int GetHashCode() const
		{
            return (int)_ticks ^ (int)(_ticks >> 32);
        }

        static ScopeTimeSpan FromHours(double value) 
		{
            return Interval(value, MillisPerHour);
        }
        
		static ScopeTimeSpan Interval(double value, int scale) 
		{
            if (_isnan(value))
                throw std::invalid_argument("argument nan");
            double tmp = value * scale;
            double millis = tmp + (value >= 0? 0.5: -0.5);
            if ((millis > MaxMilliSeconds) || (millis < MinMilliSeconds))
                throw std::invalid_argument("argument out of set range");
            return ScopeTimeSpan((__int64)millis * TicksPerMillisecond);
        }
        
		static ScopeTimeSpan FromMilliseconds(double value) 
		{
            return Interval(value, 1);
		}
        
		static ScopeTimeSpan FromMinutes(double value) 
		{
            return Interval(value, MillisPerMinute);
        }
        
		ScopeTimeSpan Negate() const
		{
            if (Ticks()==MinTicks)
                throw std::invalid_argument("argument out of supporting range");
            return ScopeTimeSpan(-_ticks);
        }
        
		static ScopeTimeSpan FromSeconds(double value) 
		{
            return Interval(value, MillisPerSecond);
        }
        
		ScopeTimeSpan Subtract(const ScopeTimeSpan& ts) const
		{
            __int64 result = _ticks - ts._ticks;
            // Overflow if signs of operands was different and result's
            // sign was opposite from the first argument's sign.
            // >> 63 gives the sign bit (either 64 1's or 64 0's).
            if (((_ticks & 0x8000000000000000) != (ts._ticks & 0x8000000000000000)) && 
				((_ticks & 0x8000000000000000) != (result & 0x8000000000000000)))
                throw std::invalid_argument("argument out of supporting range");
            return ScopeTimeSpan(result);
        }
        
		static ScopeTimeSpan FromTicks(__int64 value) 
		{
            return ScopeTimeSpan(value);
        }
        
		static __int64 TimeToTicks(int hour, int minute, int second) 
		{
            // totalSeconds is bounded by 2^31 * 2^12 + 2^31 * 2^8 + 2^31,
            // which is less than 2^44, meaning we won't overflow totalSeconds.
            __int64 totalSeconds = (__int64)hour * 3600 + (__int64)minute * 60 + (__int64)second;
            if (totalSeconds > MaxSeconds || totalSeconds < MinSeconds)
                throw std::invalid_argument("argument out of supporting range");
            return totalSeconds * TicksPerSecond;
        }

        ScopeTimeSpan operator -() const
		{
			if(_ticks == MinTicks)
                throw std::invalid_argument("argument out of supporting range");
            return ScopeTimeSpan(-_ticks);
        }
        
		ScopeTimeSpan operator -(const ScopeTimeSpan& t) const
		{
            return Subtract(t);
        }
        
		ScopeTimeSpan operator +() const
		{
            return *this;
        }
        
		ScopeTimeSpan operator +(const ScopeTimeSpan& t) const
		{
            return Add(t);
        }
        
		bool operator ==(const ScopeTimeSpan& t) const
		{
			return Equals(*this, t);
        }
        
		bool operator !=(const ScopeTimeSpan& t) const
		{
			return !Equals(*this, t);
        }
        
		bool operator <(const ScopeTimeSpan& t) const
		{
            return _ticks < t._ticks;
        }
        
		bool operator <=(const ScopeTimeSpan& t) const
		{
            return _ticks <= t._ticks;
        }
        
		bool operator >(const ScopeTimeSpan& t) const
		{
            return _ticks > t._ticks;
        }
        
		bool operator >=(const ScopeTimeSpan& t) const
		{
            return _ticks >= t._ticks;
        }
	};
}
