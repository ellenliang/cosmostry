#pragma once

// Enable this code once generated C++ files are split
#ifndef __cplusplus_cli
#error "Managed scope operators must be compiled with /clr"
#endif

#include "ScopeContainers.h"
#include "ScopeIO.h"

#include <string>

#include <msclr/appdomain.h>
#include <msclr\marshal.h>
#include <msclr\marshal_cppstd.h>

using namespace System;
using namespace System::Collections;
using namespace System::Reflection;
using namespace Microsoft::SCOPE::Interfaces;

// workaround common identifiers that clash between a C# and C++
#undef IN
#undef OUT
#undef OPTIONAL

#define SCOPECEP_CHECKPINT_MAGICNUMBER 0x12345678

namespace ScopeEngine
{
    // Initilalize/finalize managed runtime
    extern INLINE void InitializeScopeRuntime(std::string * argv, int argc)
    {
        ScopeRuntime::Global::GCMemoryWatcher->Initialize();
        ScopeRuntime::RuntimeConfiguration::Initialize(___Scope_Generated_Classes___::__UdtTypeTable__);
        ScopeRuntime::ScopeTrace::AddListener(gcnew System::Diagnostics::ConsoleTraceListener());

        // Register Debug Streams from command line arguments

        array<String^>^ arr_args = gcnew array<String^>(argc){};
        for(int i=0; i<argc; ++i)
        {
            String^ args = gcnew String(argv[i].c_str());
            arr_args[i] = ScopeRuntime::EscapeStrings::UnEscape(args);
        }
        
        ScopeRuntime::Global::DebugStreamManager->RegisterDebugStreams(arr_args);
    }

    extern INLINE void FinalizeScopeRuntime(UINT64& peakManagedMemory)
    {
        ScopeRuntime::ScopeTrace::RemoveAllListeners();
        ScopeRuntime::Global::DebugStreamManager->FinalizeDebugStreams();
        peakManagedMemory = (UINT64)ScopeRuntime::Global::GCMemoryWatcher->PeakGCMemory;
        ScopeRuntime::Global::GCMemoryWatcher->Shutdown();
    }

#pragma region ManagedHandle
    //
    // Following code is managed implementation of a handle that is safe to declare in native code, thus
    // enabling us to have separate native and managed compilation in the future.
    //
    typedef System::Runtime::InteropServices::GCHandle GCHandle;

    INLINE void CheckScopeCEPCheckpointMagicNumber(System::IO::BinaryReader^ checkpoint)
    {
        int magic = checkpoint->ReadInt32();
        if (magic != SCOPECEP_CHECKPINT_MAGICNUMBER)
        {
            throw gcnew InvalidOperationException("Load Checkpoint overflow or underflow");
        }
    }

    template <typename T>
    ScopeManagedHandle::ScopeManagedHandle(T t)
    {
        SCOPE_ASSERT(sizeof(m_handle) == sizeof(GCHandle));
        GCHandle h = GCHandle::Alloc(t);
        memcpy(&m_handle, &h, sizeof(GCHandle));
    }

    template <typename T>
    ScopeManagedHandle::operator T () const
    {
        if (m_handle != NULL)
        {
            // Keep the const-ness on the function and cast it away here since we are still not modifying any memory
            return static_cast<T>(reinterpret_cast<GCHandle *>(const_cast<void **>(&m_handle))->Target);
        }
        else
        {
            return static_cast<T>(nullptr);
        }
    }

    template <typename T>
    ScopeManagedHandle& ScopeManagedHandle::operator=(T t)
    {
        SCOPE_ASSERT(m_handle == NULL);
        GCHandle h = GCHandle::Alloc(t);
        memcpy(&m_handle, &h, sizeof(GCHandle));
        return *this;
    }

    template <typename T>
    T scope_handle_cast(const ScopeManagedHandle & handle)
    {
        return static_cast<T>(handle);
    }

    template<typename T>
    class ScopeTypedManagedHandle
    {
    public:
        ScopeTypedManagedHandle()
        {
        }

        explicit ScopeTypedManagedHandle(T t) : m_handle(t)
        {
        }

        ScopeTypedManagedHandle& operator=(T t)
        {
            m_handle = t;
            return *this;
        }

        operator T () const
        {
            return scope_handle_cast<T>(m_handle);
        }

        T operator->() const
        {
            return scope_handle_cast<T>(m_handle);
        }

        T get() const
        {
            return scope_handle_cast<T>(m_handle);
        }

        void reset()
        {
            m_handle.reset();
        }

        void reset(T t)
        {
            reset();

            if (t != nullptr)
            {
                m_handle = t;
            }
        }

    private:
        ScopeManagedHandle m_handle;
    };
#pragma endregion ManagedHandle

    // Wraper for Row object to be used in native code
    template<typename Schema>
    struct ManagedRow
    {
        ScopeRuntime::Row ^ get();
        System::Object^ ComplexColumnGetter(int index, BYTE* address);
    };

    // Wrapper for UDO to be used in native code
    template<int UID>
    struct ManagedUDO
    {
        System::Object^ get();
        System::Collections::Generic::List<System::String^>^ args();
    };

    // Wrapper for UDT to be used in native code
    template<int ColumnTypeID>
    struct ManagedUDT
    {
        typedef void Typename;
        ScopeRuntime::ColumnData^ get();
    };

    template<typename Schema>
    INLINE void ManagedRowFactory<Schema>::Create(ManagedRow<Schema> * schema)
    {
        // We have allocated fixed size for ManagedRow<Schema>, pointer sized, so any change in size
        // of ManagedRow will have to revisit this code.
        static_assert(sizeof(ManagedRow<Schema>) == sizeof(void *), "Size of ManagedRow class grew beyond pointer size. Please revise this code!");
        new ((char *) schema) ManagedRow<Schema>();
    }

    template<int SchemaId>
    struct ManagedSStreamSchema
    {
        StructuredStream::StructuredStreamSchema^ get();
    };

    template<int SchemaId>
    INLINE ScopeSStreamSchemaStatic<SchemaId>::ScopeSStreamSchemaStatic()
    {
        m_managed = ManagedSStreamSchema<SchemaId>().get();
    }

    class ScopeUDTColumnTypeHelper
    {
    public:
        // Check if UDT holds null value
        static bool IsNull(const ScopeUDTColumnType & columnType)
        {
            ScopeRuntime::ColumnData ^ columnData = scope_handle_cast<ScopeRuntime::ColumnData ^>(columnType.m_managed);
            SCOPE_ASSERT(columnData != nullptr);

            return columnData->IsNull();
        }

        // Set UDT to null value
        static void SetNull(ScopeUDTColumnType & columnType)
        {
            ScopeRuntime::ColumnData ^ columnData = scope_handle_cast<ScopeRuntime::ColumnData ^>(columnType.m_managed);
            SCOPE_ASSERT(columnData != nullptr);

            columnData->SetNull();
        }

        // Get and release the reference to the embedded object of the columndata
        static System::Object^ GetColumnObject(const ScopeUDTColumnType & columnType)
        {
            ScopeRuntime::ColumnData ^ columnData = scope_handle_cast<ScopeRuntime::ColumnData ^>(columnType.m_managed);
            SCOPE_ASSERT(columnData != nullptr);

            return columnData->Value;
        }

        // Get the reference to the embedded object of the columndata
        static System::Object^ GetAndReleaseColumnObject(ScopeUDTColumnType & columnType)
        {
            ScopeRuntime::ColumnData ^ columnData = scope_handle_cast<ScopeRuntime::ColumnData ^>(columnType.m_managed);
            SCOPE_ASSERT(columnData != nullptr);

            System::Object ^ tmp = columnData->Value;

            columnData->Reset();

            return tmp;
        }

        // Get and release the reference to the embedded object of the columndata
        static void SetColumnObject(ScopeUDTColumnType & columnType, System::Object^ o)
        {
            ScopeRuntime::ColumnData ^ columnData = scope_handle_cast<ScopeRuntime::ColumnData ^>(columnType.m_managed);
            SCOPE_ASSERT(columnData != nullptr);

            columnData->Set(o);
        }

        // deserialize the UDT from a string
        static void Deserialize(ScopeUDTColumnType & columnType, System::String^ token)
        {
            ScopeRuntime::ColumnData ^ columnData = scope_handle_cast<ScopeRuntime::ColumnData ^>(columnType.m_managed);
            SCOPE_ASSERT(columnData != nullptr);

            columnData->UnsafeSet(token);
        }

        // deserialize the UDT from a binary reader stream
        static void Deserialize(ScopeUDTColumnType & columnType, System::IO::BinaryReader^ stream)
        {
            ScopeRuntime::ColumnData ^ columnData = scope_handle_cast<ScopeRuntime::ColumnData ^>(columnType.m_managed);
            SCOPE_ASSERT(columnData != nullptr);

            columnData->Deserialize(stream);
        }

        static void SSLibDeserializeUDT(ScopeUDTColumnType & columnType, array<Byte>^ buffer, int offset, int length, ScopeSStreamSchema& schema)
        {
            ScopeRuntime::ColumnData ^ columnData = scope_handle_cast<ScopeRuntime::ColumnData ^>(columnType.m_managed);
            SCOPE_ASSERT(columnData != nullptr);

            StructuredStream::StructuredStreamSchema^ sschema = scope_handle_cast<StructuredStream::StructuredStreamSchema^>(schema.ManagedSchema());
            SCOPE_ASSERT(sschema != nullptr);

            columnData->SSLIBFastDeserializeV3(buffer, offset, length, sschema);
        }

        static void SSLibSerializeUDT(const ScopeUDTColumnType & columnType, System::IO::Stream^ stream)
        {
            ScopeRuntime::ColumnData ^ columnData = scope_handle_cast<ScopeRuntime::ColumnData ^>(columnType.m_managed);
            SCOPE_ASSERT(columnData != nullptr);

            columnData->SSLIBFastSerializeV3(stream);
        }

        // Serialize the UDT to a BinaryWriter
        static void Serialize(const ScopeUDTColumnType & columnType, System::IO::BinaryWriter^ stream)
        {
            ScopeRuntime::ColumnData ^ columnData = scope_handle_cast<ScopeRuntime::ColumnData ^>(columnType.m_managed);
            SCOPE_ASSERT(columnData != nullptr);

            columnData->Serialize(stream);
        }

        // Serialize the UDT to a StreamWriter
        static void Serialize(const ScopeUDTColumnType & columnType, System::IO::StreamWriter^ stream)
        {
            ScopeRuntime::ColumnData ^ columnData = scope_handle_cast<ScopeRuntime::ColumnData ^>(columnType.m_managed);
            SCOPE_ASSERT(columnData != nullptr);

            columnData->Serialize(stream);
        }
    };

    // Do a shallow copy of UDT object in ColumnData 
    extern INLINE void CreateScopeUDTObject(ScopeManagedHandle & managedHandle, int udtId)
    {
        System::Type ^ udtType = ___Scope_Generated_Classes___::__UdtTypeTable__[udtId];

        // TODO: check if I can construct object faster
        managedHandle = System::Activator::CreateInstance(udtType);
    }

    // Do a shallow copy of UDT object in ColumnData 
    extern INLINE void CopyScopeUDTObject(const ScopeUDTColumnType & src, ScopeUDTColumnType & dest)
    {
        ScopeUDTColumnTypeHelper::SetColumnObject(dest, ScopeUDTColumnTypeHelper::GetColumnObject(src));
    }

    // Check if UDT is null
    extern INLINE bool CheckNullScopeUDTObject(const ScopeUDTColumnType & udt)
    {
        return ScopeUDTColumnTypeHelper::IsNull(udt);
    }

    // Set UDT to null
    extern INLINE void SetNullScopeUDTObject(ScopeUDTColumnType & udt)
    {
        ScopeUDTColumnTypeHelper::SetNull(udt);
    }

    // Parse UDT using IScopeSerializableText interface defined on the UDT.
    extern INLINE void FStringToScopeUDTColumnType(const char * str, ScopeUDTColumnType & out)
    {
        try
        {
            System::String^ token = gcnew System::String(str);
            ScopeUDTColumnTypeHelper::Deserialize(out, token);
        }
        catch(System::Exception^)
        {
            throw ScopeStreamException(ScopeStreamException::BadFormat);
        }
    }

    template<int ColumnTypeID>
    INLINE ScopeUDTColumnTypeStatic<ColumnTypeID>::ScopeUDTColumnTypeStatic()
    {
        ManagedUDT<ColumnTypeID> udt;
        m_managed = udt.get();
        m_udtId = ColumnTypeID;
    }

    template<int ColumnTypeID>
    template<typename OtherType>
    INLINE ScopeUDTColumnTypeStatic<ColumnTypeID>::ScopeUDTColumnTypeStatic(const OtherType & o)
    {
        m_managed = ManagedUDT<ColumnTypeID>().get();
        m_udtId = ColumnTypeID;

        // point the columndata to the same UDT object.
        scope_handle_cast<ScopeRuntime::ColumnData ^>(m_managed)->Set(static_cast<decltype(scope_handle_cast<ScopeRuntime::ColumnData ^>(m_managed)->Value)>(o));
    }

    template<int ColumnTypeID>
    INLINE ScopeUDTColumnTypeStatic<ColumnTypeID>::ScopeUDTColumnTypeStatic(const ScopeUDTColumnTypeStatic<ColumnTypeID> & c)
    {
        m_managed = ManagedUDT<ColumnTypeID>().get();
        m_udtId = ColumnTypeID;

        // point the columndata to the same UDT object.
        scope_handle_cast<ScopeRuntime::ColumnData ^>(m_managed)->Set(scope_handle_cast<ScopeRuntime::ColumnData ^>(c.m_managed)->Value);
    }

    template<int ColumnTypeID>
    template<typename CastType>
    INLINE ScopeUDTColumnTypeStatic<ColumnTypeID>::operator CastType() const
    {      
        return static_cast<CastType>(scope_handle_cast<ScopeRuntime::ColumnData ^>(m_managed)->Value);    
    }

    //
    // Special allocator for the Native->Managed Interop
    //
    class InteropAllocator : public IncrementalAllocator
    {
        std::vector<ULONGLONG> m_objectId;
        ULONGLONG m_nextObjectId;

        ULONGLONG GetObjectId(int pos) const
        {
            SCOPE_ASSERT(pos < m_objectId.size());

            return m_objectId[pos];
        }

        ULONGLONG SetObjectId(int pos)
        {
            SCOPE_ASSERT(pos < m_objectId.size());

            return (m_objectId[pos] = ++m_nextObjectId);
        }

        void ResetObjectId(int pos)
        {
            SCOPE_ASSERT(pos < m_objectId.size());

            m_objectId[pos] = 0;
        }

#ifndef SCOPE_INTEROP_NOOPT
        template<typename T>
        bool NeedsCopying(T & obj, int columnId)
        {
            bool doCopy = true;

            USHORT allocId;
            ULONG  allocVersion;
            ULONGLONG destId;

            if (obj.GetSharedPtrInfo(allocId, allocVersion, destId))
            {
                // Ignore allocator version
                if (allocId == Id() && destId == GetObjectId(columnId))
                {
                    // Everything matches, may re-use managed object
                    doCopy = false;
                }
                else
                {
                    ULONGLONG objectId = SetObjectId(columnId);

                    // Update shared pointer and allocator's object table with objectId
                    bool succeed = obj.SetSharedPtrInfo(Id(), Version(), objectId);
                    SCOPE_ASSERT(succeed);
                }
            }
            else
            {
                ResetObjectId(columnId);
            }

            return doCopy;
        }
#else
        template<typename T>
        bool NeedsCopying(T &, int)
        {
            return true;
        }
#endif

     public:
 
        static array<System::Byte>^ CopyToManagedBuffer(const void * buf, int size)
        {
            SCOPE_ASSERT(size >= 0);
            array<System::Byte>^ managedBuf = gcnew array<System::Byte>(size);
            
            if (size)
            {
                pin_ptr<System::Byte> pinnedBuf = &managedBuf[0];
                memcpy(pinnedBuf, buf, size);
            }

            return managedBuf;
        }

        static System::IO::UnmanagedMemoryStream^ CreateMemoryStream(const void * cbuf, int size)
        {
            SCOPE_ASSERT(size >= 0);
            void* buf = const_cast<void*>(cbuf);
            System::IO::UnmanagedMemoryStream^ stream = gcnew System::IO::UnmanagedMemoryStream(reinterpret_cast<unsigned char*>(buf), size, size, System::IO::FileAccess::Read);

            return stream;
        }

        InteropAllocator(int schemaSize, const char* ownerName) : IncrementalAllocator(1, ownerName), m_nextObjectId(0)
        {
            m_objectId.resize(schemaSize, 0);
        }

        template<typename T>
        void CopyToManagedColumn(T & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int)
        {
            managedColumn->Set(nativeColumn);
        }

        template<>
        void CopyToManagedColumn(FString & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int columnId)
        {
            if (NeedsCopying(nativeColumn, columnId))
            {
                if (nativeColumn.IsNull())
                {
                    managedColumn->SetNull();
                }
                else
                {
                    managedColumn->Set(CopyToManagedBuffer(nativeColumn.buffer(), nativeColumn.size()));
                }
            }
        }

        template<>
        void CopyToManagedColumn(FBinary & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int columnId)
        {
            if (NeedsCopying(nativeColumn, columnId))
            {
                if (nativeColumn.IsNull())
                {
                    managedColumn->SetNull();
                }
                else
                {
                    if (nativeColumn.buffer())
                    {
                        managedColumn->Set(CreateMemoryStream(nativeColumn.buffer(), nativeColumn.size()));
                    }
                    else
                    {
                        managedColumn->Set(CopyToManagedBuffer(nativeColumn.buffer(), nativeColumn.size()));
                    }
                }
            }
        }

        template<typename T>
        void CopyToManagedColumn(NativeNullable<T> & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int columnId)
        {
            if (nativeColumn.IsNull())
            {
                managedColumn->SetNull();
            }
            else
            {
                CopyToManagedColumn(nativeColumn.get(), managedColumn, columnId);
            }
        }

        void CopyToManagedColumn(ScopeUDTColumnTypeDynamic & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int)
        {
            managedColumn->Set(ScopeUDTColumnTypeHelper::GetAndReleaseColumnObject(nativeColumn));
        }

        template<int ColumnTypeID>
        void CopyToManagedColumn(ScopeUDTColumnTypeStatic<ColumnTypeID> & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int)
        {
            managedColumn->Set(ScopeUDTColumnTypeHelper::GetAndReleaseColumnObject(nativeColumn));
        }

        template<>
        void CopyToManagedColumn<ScopeDateTime>(ScopeDateTime & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int)
        {
            managedColumn->Set(System::DateTime::FromBinary(nativeColumn.ToBinary()));
        }
        
        template<>
        void CopyToManagedColumn<ScopeDecimal>(ScopeDecimal & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int)
        {
            managedColumn->Set(System::Decimal(nativeColumn.Lo32Bit(), nativeColumn.Mid32Bit(), nativeColumn.Hi32Bit(), nativeColumn.Sign()>0, (unsigned char)nativeColumn.Scale()));
        }

        template<>
        void CopyToManagedColumn<ScopeGuid>(ScopeGuid & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int)
        {
            GUID guid = nativeColumn.get();
            managedColumn->Set(gcnew System::Guid(guid.Data1, guid.Data2, guid.Data3, guid.Data4[0], guid.Data4[1], guid.Data4[2], guid.Data4[3],
                guid.Data4[4], guid.Data4[5],guid.Data4[6], guid.Data4[7]));
        }

        template<typename NK, typename NV, typename MK, typename MV>
        void CopyToManagedColumn(ScopeEngine::ScopeMapNative<NK,NV> & nativeColumn, ScopeRuntime::MapColumnData<MK,MV>^ managedColumn, int)
        {
            Microsoft::SCOPE::Types::ScopeMap<MK,MV>^ scopeMapManaged = nullptr;
            ScopeManagedInterop::CopyToManagedColumn(nativeColumn, scopeMapManaged);
            managedColumn->Set(scopeMapManaged);
        }

        template<typename NT, typename MT>
        void CopyToManagedColumn(ScopeEngine::ScopeArrayNative<NT> & nativeColumn, ScopeRuntime::ArrayColumnData<MT>^ managedColumn, int)
        {
            Microsoft::SCOPE::Types::ScopeArray<MT>^ scopeArrayManaged = nullptr;
            ScopeManagedInterop::CopyToManagedColumn(nativeColumn, scopeArrayManaged);
            managedColumn->Set(scopeArrayManaged);            
        }
        
        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("InteropAllocator");
            node.AddAttribute(RuntimeStats::MaxCommittedSize(), m_objectId.capacity() * sizeof(ULONGLONG));
            node.AddAttribute(RuntimeStats::AvgCommittedSize(), m_objectId.capacity() * sizeof(ULONGLONG));
            IncrementalAllocator::WriteRuntimeStats(node);
        }
    };

    class ScopeManagedInterop
    {
    public:
        template<typename T, typename MT>
        static INLINE void CopyToNativeColumn(T & nativeColumn, MT managedColumn, IncrementalAllocator *)
        {
            nativeColumn = managedColumn;
        }

        template<>
        static INLINE void CopyToNativeColumn<FString,array<System::Byte>^>(FString & nativeColumn, array<System::Byte> ^ managedColumn, IncrementalAllocator * alloc)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
            }
            else
            {
                int size = managedColumn->Length;
                if (size)
                {
                    pin_ptr<System::Byte> bufsrc = &managedColumn[0];
                    nativeColumn.CopyFrom((char*)bufsrc, size, alloc);
                }
                else
                {
                    nativeColumn.SetEmpty();
                }
            }
        }

        template<>
        static INLINE void CopyToNativeColumn<FString, String^>(FString & nativeColumn, String^ managedColumn, IncrementalAllocator * alloc)
        {
            array<System::Byte>^ buffer = ScopeRuntime::StringColumnData::StringToBytes(managedColumn);
            CopyToNativeColumn(nativeColumn, buffer, alloc);
        }
        
        template<>
        static INLINE void CopyToNativeColumn<FBinary,array<System::Byte>^>(FBinary & nativeColumn, array<System::Byte> ^ managedColumn, IncrementalAllocator * alloc)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
            }
            else
            {
                int size = managedColumn->Length;
                if (size)
                {
                    pin_ptr<System::Byte> bufsrc = &managedColumn[0];
                    nativeColumn.CopyFrom((unsigned char*)bufsrc, size, alloc);
                }
                else
                {
                    nativeColumn.SetEmpty();
                }
            }
        }

        template<typename T, typename MT>
        static INLINE void CopyToNativeColumn(NativeNullable<T> & nativeColumn, MT managedColumn, IncrementalAllocator * alloc)
        {
            if (!managedColumn.HasValue)
            {
                nativeColumn.SetNull();
            }
            else
            {
                CopyToNativeColumn(nativeColumn.get(), managedColumn.Value, alloc);
                nativeColumn.ClearNull();
            }
        }

        template<typename MT>
        static INLINE void CopyToNativeColumn(ScopeUDTColumnTypeDynamic & nativeColumn, MT managedColumn, IncrementalAllocator *)
        {
            ScopeUDTColumnTypeHelper::SetColumnObject(nativeColumn, managedColumn);
        }

        template<int ColumnTypeID, typename MT>
        static INLINE void CopyToNativeColumn(ScopeUDTColumnTypeStatic<ColumnTypeID> & nativeColumn, MT managedColumn, IncrementalAllocator *)
        {
            ScopeUDTColumnTypeHelper::SetColumnObject(nativeColumn, managedColumn);
        }

        template<>
        static INLINE void CopyToNativeColumn<ScopeDateTime, System::DateTime>(ScopeDateTime & nativeColumn, System::DateTime managedColumn, IncrementalAllocator *)
        {
            nativeColumn = ScopeDateTime::FromBinary(managedColumn.ToBinary());
        }

        template<>
        static INLINE void CopyToNativeColumn<ScopeDecimal,System::Decimal>(ScopeDecimal & nativeColumn, System::Decimal managedColumn, IncrementalAllocator *)
        {
            array<int>^Bits = System::Decimal::GetBits(managedColumn);

            nativeColumn.Reset(Bits[2], Bits[1], Bits[0], Bits[3]);
        }

        template<>
        static INLINE void CopyToNativeColumn<ScopeGuid,System::Guid>(ScopeGuid & nativeColumn, System::Guid managedColumn, IncrementalAllocator *)
        {
            array<System::Byte>^ buffer = managedColumn.ToByteArray();
            pin_ptr<System::Byte> bufsrc = &buffer[0];

            nativeColumn.CopyFrom((unsigned char*)bufsrc);
        }

        template<typename NK, typename NV, typename MK, typename MV>
        static void CopyToNativeColumn(ScopeEngine::ScopeMapNative<NK,NV> & nativeColumn, Microsoft::SCOPE::Types::ScopeMap<MK,MV>^ managedColumn, IncrementalAllocator * alloc)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
                return;
            }
            
            ScopeMapWrapper<NK, NV, MK, MV>^ wrapper = dynamic_cast<ScopeMapWrapper<NK, NV, MK, MV>^>(managedColumn);
            if (wrapper != nullptr)
            {
                nativeColumn = *(wrapper->GetScopeMapNative());          
            }
            else
            {
                // we had some content created inside udo and never had a chance to hook with native scope map, copy here.
                nativeColumn.Reset(alloc);           
                NK nkey;
                NV nvalue; 
                for each(Generic::KeyValuePair<MK, MV>^ kv in managedColumn)
                {  
                    ScopeManagedInterop::CopyToNativeColumn(nkey, kv->Key, alloc);
                    ScopeManagedInterop::CopyToNativeColumn(nvalue, kv->Value, alloc);                
                    nativeColumn.Add(nkey, nvalue);
                }                
            }
        }  

        template<typename NT, typename MT>
        static void CopyToNativeColumn(ScopeEngine::ScopeArrayNative<NT> & nativeColumn, Microsoft::SCOPE::Types::ScopeArray<MT>^ managedColumn, IncrementalAllocator * alloc)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
                return;
            }
            
            ScopeArrayWrapper<NT, MT>^ wrapper = dynamic_cast<ScopeArrayWrapper<NT, MT>^>(managedColumn);
            if (wrapper != nullptr)
            {
                nativeColumn = *(wrapper->GetScopeArrayNative());
            }           
            else
            {
                // we had some content created inside udo and never had a chance to hook with native scope array, copy here.
                nativeColumn.Reset(alloc);           
                NT nvalue; 
                for each(MT mvalue in managedColumn)
                {  
                    ScopeManagedInterop::CopyToNativeColumn(nvalue, mvalue, alloc);                
                    nativeColumn.Append(nvalue);
                }                
            }
        }  

        template<typename NT, typename MT>
        static INLINE void CopyToManagedColumn(const NT & nativeColumn, MT % managedColumn)
        {
            managedColumn = nativeColumn;
        }  
               
        static INLINE void CopyToManagedColumn(const FString & nativeColumn, System::String^ % managedColumn)
        {
            if (nativeColumn.IsNull())
            {
                managedColumn = nullptr;
            }
            else
            {
                managedColumn = ScopeRuntime::StringColumnData::BytesToString(
                                InteropAllocator::CopyToManagedBuffer(nativeColumn.buffer(), nativeColumn.size()));
            }
        }     
                
        static INLINE void CopyToManagedColumn(const FBinary & nativeColumn, array<System::Byte>^ % managedColumn)
        {
            if (nativeColumn.IsNull())
            {
                managedColumn = nullptr;
            }
            else
            {
                managedColumn = InteropAllocator::CopyToManagedBuffer(nativeColumn.buffer(), nativeColumn.size());
            }            
        }   
        
        template<typename NT, typename MT>
        static INLINE void CopyToManagedColumn(const NativeNullable<NT> & nativeColumn, MT % managedColumn)
        {        
            if (nativeColumn.IsNull())
            {
                managedColumn = MT(); // set null
            }
            else
            {
                CopyToManagedColumn(nativeColumn.get(), managedColumn);
            }            
        }     
        
        static INLINE void CopyToManagedColumn(const ScopeDateTime & nativeColumn, System::DateTime % managedColumn)
        {
            managedColumn = System::DateTime::FromBinary(nativeColumn.ToBinary());
        }
       
        static INLINE void CopyToManagedColumn(const ScopeDecimal & nativeColumn, System::Decimal % managedColumn)
        {
            managedColumn = System::Decimal(nativeColumn.Lo32Bit(), 
                                            nativeColumn.Mid32Bit(), 
                                            nativeColumn.Hi32Bit(), 
                                            nativeColumn.Sign() > 0, 
                                            (unsigned char)nativeColumn.Scale());
        }

        static INLINE void CopyToManagedColumn(const ScopeGuid & nativeColumn, System::Guid % managedColumn)
        {
            GUID guid = nativeColumn.get();
            managedColumn = System::Guid(guid.Data1, guid.Data2, guid.Data3, 
                                         guid.Data4[0], guid.Data4[1], guid.Data4[2], guid.Data4[3],
                                         guid.Data4[4], guid.Data4[5], guid.Data4[6], guid.Data4[7]);
        }

        static INLINE void CopyToManagedColumn(const ScopeDateTime & nativeColumn, System::Nullable<System::DateTime> % managedColumn)
        {
            managedColumn = System::DateTime::FromBinary(nativeColumn.ToBinary());
        }
       
        static INLINE void CopyToManagedColumn(const ScopeDecimal & nativeColumn, System::Nullable<System::Decimal> % managedColumn)
        {
            managedColumn = System::Decimal(nativeColumn.Lo32Bit(), 
                                            nativeColumn.Mid32Bit(), 
                                            nativeColumn.Hi32Bit(), 
                                            nativeColumn.Sign() > 0, 
                                            (unsigned char)nativeColumn.Scale());
        }

        static INLINE void CopyToManagedColumn(const ScopeGuid & nativeColumn, System::Nullable<System::Guid> % managedColumn)
        {
            GUID guid = nativeColumn.get();
            managedColumn = System::Guid(guid.Data1, guid.Data2, guid.Data3, 
                                         guid.Data4[0], guid.Data4[1], guid.Data4[2], guid.Data4[3],
                                         guid.Data4[4], guid.Data4[5], guid.Data4[6], guid.Data4[7]);
        }

        template<typename NK, typename NV, typename MK, typename MV>
        static INLINE void CopyToManagedColumn(const ScopeEngine::ScopeMapNative<NK,NV>& nativeColumn, Microsoft::SCOPE::Types::ScopeMap<MK,MV>^ % managedColumn)
        {
            if (nativeColumn.IsNull())
            {
                managedColumn = nullptr;
                return;
            }
            
            /// Copy to managed columns's manage space, did not hook native map.
            /// TODO: optimze to whole buffer copy (native serialize and then manage deserialize) if there are many key/valuses 
            Generic::Dictionary<MK, MV>^ buffer = gcnew Generic::Dictionary<MK, MV>();
            MK mkey;
            MV mvalue;
            for(auto iter = nativeColumn.begin(); iter != nativeColumn.end(); ++iter)
            {
                ScopeManagedInterop::CopyToManagedColumn(iter.Key(), mkey);
                ScopeManagedInterop::CopyToManagedColumn(iter.Value(), mvalue);
                buffer[mkey] = mvalue;
            }

            managedColumn = gcnew Microsoft::SCOPE::Types::ScopeMap<MK,MV>(buffer);

        }  

        template<typename NT, typename MT>
        static INLINE void CopyToManagedColumn(const ScopeEngine::ScopeArrayNative<NT>& nativeColumn, Microsoft::SCOPE::Types::ScopeArray<MT>^ % managedColumn)
        {
            if (nativeColumn.IsNull())
            {
                managedColumn = nullptr;
                return;
            }
            
            Generic::List<MT>^ buffer = gcnew Generic::List<MT>();
            MT mvalue;
            for(auto iter = nativeColumn.begin(); iter != nativeColumn.end(); ++iter)
            {
                ScopeManagedInterop::CopyToManagedColumn(iter.Value(), mvalue);
                buffer->Add(mvalue);
            }
            
            managedColumn = gcnew Microsoft::SCOPE::Types::ScopeArray<MT>(buffer);
        }
    };

    // Marshal a native row to a managed row
    template<typename RowSchema, int UID>
    class InteropToManagedRowPolicy
    {
    public:
        static void Marshal(RowSchema& nativeRow, ScopeRuntime::Row^ managedRow, InteropAllocator * alloc);

        static void Marshal(RowSchema& nativeRow, typename KeyComparePolicy<RowSchema,UID>::KeyType& nativeKey, ScopeRuntime::Row^ managedRow, InteropAllocator * alloc);
    };

    // Marshal a managed row to a native row
    template<typename RowSchema, int>
    class InteropToNativeRowPolicy
    {
    public:
        static void Marshal(ScopeRuntime::Row^ managedRow, RowSchema& nativeRow, IncrementalAllocator* alloc);
        static void Marshal(IRow^ managedRow, RowSchema& nativeRow, IncrementalAllocator* alloc);
    };

    ref class ScopeNativeExceptionHelper abstract sealed
    {    
    public:       
        static ScopeRuntime::ScopeNativeException^ WrapNativeException(const std::exception &e)
        {            
            if (auto exception = dynamic_cast<const RuntimeException*>(&e))
            {
                return CreateScopeNativeException(*exception);
            }

            if (auto exception = dynamic_cast<const ExceptionWithStack*>(&e))
            {
                return CreateScopeNativeException(*exception);
            }

            return CreateScopeNativeException(e);
        }        

    private:
        static ScopeRuntime::ScopeNativeException^ CreateScopeNativeException(const ExceptionWithStack &e)
        {
            return CreateScopeNativeException(e, e.GetErrorNumber(), e.GetDetails(), e.GetStack());
        }

        static ScopeRuntime::ScopeNativeException^ CreateScopeNativeException(const RuntimeException &e)
        {
            return CreateScopeNativeException(e, e.GetErrorNumber(), e.what(), e.GetStack());
        }

        static ScopeRuntime::ScopeNativeException^ CreateScopeNativeException(const std::exception &e)
        {
            return CreateScopeNativeException(e, E_USER_ERROR, e.what(), "");
        }

        static ScopeRuntime::ScopeNativeException^ CreateScopeNativeException(
            const std::exception &e, 
            ErrorNumber errorNumber, 
            std::string description, 
            std::string stackTrace)
        {
            System::UInt32 nativeErrorNumber = (System::UInt32) errorNumber;
            System::String^ nativeExceptionType = gcnew System::String(typeid(e).name());
            System::String^ nativeDescription = gcnew System::String(description.c_str());
            System::String^ nativeStackTrace = gcnew System::String(stackTrace.c_str());

            return gcnew ScopeRuntime::ScopeNativeException(
                nativeErrorNumber,
                nativeExceptionType,
                nativeDescription,
                nativeStackTrace);            
        }
    };


    //
    // ScopeCosmosStream implements the CLR Stream interface used by user extractors 
    ref class ScopeCosmosStream : public System::IO::Stream
    {
    protected:
        CosmosInput*    m_read;
        CosmosOutput*   m_write;

        bool            m_readOnly;
        bool            m_isOpened;
        bool            m_ownStream;

    public:
        ScopeCosmosStream(const std::string& name, bool readOnly, SIZE_T bufSize, int bufCount)
        {
            CreateStream(name, readOnly, bufSize, bufCount, true);
        }

        ScopeCosmosStream(const std::string& name, bool readOnly, SIZE_T bufSize, int bufCount, bool maintainBoundaries)
        {
            CreateStream(name, readOnly, bufSize, bufCount, maintainBoundaries);
        }

        void Init()
        {
            if (m_readOnly)
            {
                m_read->Init();
            }
            else
            {
                m_write->Init();
            }

            m_isOpened = true;
        }

        ScopeCosmosStream(BlockDevice* device, bool readOnly, SIZE_T bufSize, int bufCount)
        {
            CreateStream(device, readOnly, bufSize, bufCount, true);
        }

        ScopeCosmosStream(BlockDevice* device, bool readOnly, SIZE_T bufSize, int bufCount, bool maintainBoundaries)
        {
            CreateStream(device, readOnly, bufSize, bufCount, maintainBoundaries);
        }

        ScopeCosmosStream(CosmosInput* input, SIZE_T /*bufSize*/, int /*bufCount*/) :
            m_read(input)
        {
            m_write = nullptr;
            m_readOnly = true;
            m_isOpened = true;
            m_ownStream = false;
        }

        ScopeCosmosStream(CosmosOutput* output, SIZE_T /*bufSize*/, int /*bufCount*/) :
            m_write(output)
        {
            m_read = nullptr;
            m_readOnly = false;
            m_isOpened = true;
            m_ownStream = false;
        }

        ~ScopeCosmosStream()
        {
            this->!ScopeCosmosStream();
        }

        !ScopeCosmosStream()
        {
            if (m_ownStream)
            {
                // TODO: wrap them with smart pointers
                delete m_read;
                m_read = nullptr;

                delete m_write;
                m_write = nullptr;
            }
        }

        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                return m_readOnly;
            }         
        }

        virtual property bool CanSeek
        {
            bool get() override
            {
                // Only read-only streams are seekable
                return m_readOnly;
            }         
        }

        virtual property bool CanWrite
        {
            bool get() override
            {
                return !m_readOnly;
            }         
        }

        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                if (!m_readOnly)
                {
                    throw gcnew InvalidOperationException("Length is supported only for read-only streams");
                }

                return m_read->Length();
            }
        }

        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                if (m_readOnly)
                {
                    return m_read->GetCurrentPosition();
                }
                else
                {
                    return m_write->GetCurrentPosition();
                }
            }

            void set(LONGLONG pos) override
            {
                Seek(pos, System::IO::SeekOrigin::Begin);
            }
        }

        // The Flush actually does not flush anything. It is used to track the row boundaries. This semantics of Flush is carried over from the old managed runtime :-(
        virtual void Flush() override
        {
            if (m_readOnly)
            {
                throw gcnew InvalidOperationException("Flush is not implemented");
            }

            m_write->Commit();
        }

        virtual LONGLONG Seek(LONGLONG pos, System::IO::SeekOrigin origin) override
        {
            if (!m_readOnly)
            {
                throw gcnew InvalidOperationException("Seek is not implemented");
            }

            try 
            {
                ULONGLONG newPosition;
                switch(origin)
                {
                case System::IO::SeekOrigin::Begin: newPosition = pos;
                    break;
                case System::IO::SeekOrigin::Current: newPosition = Position + pos;
                    break;
                case System::IO::SeekOrigin::End: newPosition = Length + pos;
                    break;
                default: newPosition = numeric_limits<ULONGLONG>::max();
                    SCOPE_ASSERT(0);
                }

                if (newPosition > (ULONGLONG)Length)
                {
                    throw gcnew InvalidOperationException(
                        System::String::Format("seeking past the end, poisition= {0}, stream name={1}", 
                        newPosition, 
                        msclr::interop::marshal_as<System::String^>(GetName().c_str())));
                }

                return m_read->Seek(newPosition);
            }
            catch (std::exception& ex)
            {
                throw ScopeNativeExceptionHelper::WrapNativeException(ex);
            }
        }

        virtual void SetLength(LONGLONG) override
        {
            if (m_readOnly)
            {
                throw gcnew InvalidOperationException("SetLength is not supported for read-only streams");
            }

            throw gcnew InvalidOperationException("SetLength is not implemented");
        }

        virtual int Read(array<unsigned char>^ buffer, int offset, int count) override
        {
            try
            {
                pin_ptr<unsigned char> bufdst = &buffer[0];
                return m_read->Read((char*)(bufdst + offset), count);
            }
            catch (std::exception &e)
            {
                throw ScopeNativeExceptionHelper::WrapNativeException(e);
            }
        }

        virtual void Write(array<unsigned char>^ buffer, int offset, int count) override
        {
            if (m_readOnly)
            {
                throw gcnew InvalidOperationException("Write is not implemented");
            }
            
            try
            {
                pin_ptr<unsigned char> bufsrc = &buffer[0];
                m_write->Write((const char*)bufsrc + offset, count);
            }
            catch (std::exception &e)
            {
                throw ScopeNativeExceptionHelper::WrapNativeException(e);
            }
        }

        virtual void Close() override
        {
            if (!m_ownStream)
            {
                return;
            }

            if (!m_isOpened)
            {
                return; // it has been closed.
            }
        
            m_isOpened = false;
            if (m_read)
            {
                m_read->Close();
            }

            if (m_write)
            {
                m_write->Finish();
                m_write->Close();
            }
        }

        __int64 GetInclusiveTimeMillisecond()
        {
            if (m_read)
            {
                return m_read->GetInclusiveTimeMillisecond();
            }
            if (m_write)
            {
                return m_write->GetInclusiveTimeMillisecond();
            }
            return 0;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            if (m_read)
            {
                m_read->WriteRuntimeStats(root);
            }
            if (m_write)
            {
                m_write->WriteRuntimeStats(root);
            }
        }

        std::string GetName()
        {
            if (m_read)
            {
                return m_read->StreamName();
            }
            if (m_write)
            {
                return m_write->StreamName();
            }
            return "";
        }

        void FlushAllData()
        {
            m_write->Flush();
        }

        void SaveState(BinaryOutputStream& output, UINT64 position)
        {
            SCOPE_ASSERT(m_read);
            m_read->SaveState(output, position);
        }

        void SaveState(BinaryOutputStream& output)
        {
            SCOPE_ASSERT(m_write);
            m_write->SaveState(output);
        }

        void LoadState(BinaryInputStream& input)
        {
            if (m_read)
            {
                return m_read->LoadState(input);
            }
            if (m_write)
            {
                return m_write->LoadState(input);
            }
        }

    private:
        void CreateStream(const std::string& name, bool readOnly, SIZE_T bufSize, int bufCount, bool maintainBoundaries)
        {
            m_read = nullptr;
            m_write = nullptr;
            m_readOnly = readOnly;
            if (m_readOnly)
            {
                m_read = new CosmosInput(name, bufSize, bufCount);
            }
            else
            {
                m_write = new CosmosOutput(name, bufSize, bufCount, maintainBoundaries);
            }
            m_isOpened = false;
            m_ownStream = true;
        }

        void CreateStream(BlockDevice* device, bool readOnly, SIZE_T bufSize, int bufCount, bool maintainBoundaries)
        {
            m_read = nullptr;
            m_write = nullptr;
            m_readOnly = readOnly;
            if (m_readOnly)
            {
                m_read = new CosmosInput(device, bufSize, bufCount);
            }
            else
            {
                m_write = new CosmosOutput(device, bufSize, bufCount, maintainBoundaries);
            }
            m_isOpened = false;
            m_ownStream = true;
        }
    };

    ref class ScopeCosmosStreamWithLock : public ScopeCosmosStream
    {
        CRITICAL_SECTION* m_lock;
    public:
        ScopeCosmosStreamWithLock(const std::string& name, bool readOnly, SIZE_T bufSize, int bufCount, CRITICAL_SECTION* lock) 
            : ScopeCosmosStream(name, readOnly, bufSize, bufCount)
        {
            m_lock = lock;
        }

        virtual void Write(array<unsigned char>^ buffer, int offset, int count) override
        {
            AutoCriticalSection aCs(m_lock);
            ScopeCosmosStream::Write(buffer, offset, count);
        }
    };

    ref class ScopeCosmosStreamWithRealFlush : public ScopeCosmosStream
    {
    public:
        ScopeCosmosStreamWithRealFlush(const std::string& name, bool readOnly, SIZE_T bufSize, int bufCount)
             : ScopeCosmosStream(name, readOnly, bufSize, bufCount, false)
        {
        }

        virtual void Flush() override
        {
            try 
            {
                if (!m_readOnly)
                {
                    m_write->Flush(true);
                }
            }
            catch (std::exception &e)
            {
                throw ScopeNativeExceptionHelper::WrapNativeException(e);
            }
        }
    };
    
    ref class CosmosIOFactory : public ScopeRuntime::IOFactory
    {
    private:
        System::String^ m_base;
    public:
        CosmosIOFactory(System::String^ base)
        {
            m_base = base;
        }

        System::IO::Stream^ CreateStream(System::String^ uri, System::IO::FileMode mode, System::IO::FileAccess access) override
        {
            try
            {
                SCOPE_ASSERT(
                    (mode == System::IO::FileMode::Create && access == System::IO::FileAccess::Write) ||
                    (mode == System::IO::FileMode::Open && access == System::IO::FileAccess::Read)
                    );

                std::string path = msclr::interop::marshal_as<std::string>(m_base + "/" + uri);

                if (mode == System::IO::FileMode::Create && access == System::IO::FileAccess::Write)
                {
                    IOManager::GetGlobal()->AddOutputStream(path, path, "", ScopeTimeSpan::TicksPerWeek);
                    ScopeCosmosStream^ stream = gcnew ScopeCosmosStreamWithRealFlush(path, false, 0x400000, 2);
                    stream->Init();
                    return stream;
                }
                else
                {
                    IOManager::GetGlobal()->AddInputStream(path, path);
                    ScopeCosmosStream^ stream = gcnew ScopeCosmosStreamWithRealFlush(path, true, 0x400000, 2);
                    stream->Init();
                    return stream;
                }
            }
            catch (std::exception &e)
            {
                throw ScopeNativeExceptionHelper::WrapNativeException(e);
            }
        }
    };

    // in the managed sslib, it'll catch exception to do special handling
    // therefore, it'll lead interop if DeviceException happens
    // default exception interop wrapper will lose some information
    // this class pass the detail native exception information to managed runtime.
    // so far, it's only used in the SStreamV2Extractor wrapper
    // so, it only override Seek, Read and Close methods
    ref class ManagedSSLibScopeCosmosStream : public ScopeCosmosStream
    {
    public:
        ManagedSSLibScopeCosmosStream(BlockDevice* device, bool readOnly, SIZE_T bufSize, int bufCount)
            : ScopeCosmosStream(device, readOnly, bufSize, bufCount)
        {
            ScopeCosmosStream::Init();
        }

        virtual LONGLONG Seek(LONGLONG pos, System::IO::SeekOrigin origin) override
        {
            try
            {
                return ScopeCosmosStream::Seek(pos, origin);
            }
            catch (std::exception& ex)
            {
                throw gcnew System::IO::IOException(gcnew System::String(ex.what()));
            }
        }

        virtual int Read(array<unsigned char>^ buffer, int offset, int count) override
        {
            try
            {
                return ScopeCosmosStream::Read(buffer, offset, count);
            }
            catch (std::exception& ex)
            {
                throw gcnew System::IO::IOException(gcnew System::String(ex.what()));
            }
        }

        virtual void Close() override
        {
            try
            {
                ScopeCosmosStream::Close();
            }
            catch (std::exception& ex)
            {
                throw gcnew System::IO::IOException(msclr::interop::marshal_as<System::String^>(ex.what()));
            }
        }
    };

    //
    template<class StreamClass>
    ref class ScopeStreamWrapper : public System::IO::Stream
    {
        StreamClass * m_baseStream;
        
    public:
        ScopeStreamWrapper(StreamClass * baseStream) : m_baseStream(baseStream)
        {
        }

        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                throw gcnew InvalidOperationException("CanRead::get is not implemented");
            }
        }

        virtual property bool CanSeek
        {
            bool get() override
            {
                throw gcnew InvalidOperationException("CanSeek::get is not implemented");
            }
        }

        virtual property bool CanWrite
        {
            bool get() override
            {
                throw gcnew InvalidOperationException("CanWrite::get is not implemented");
            }
        }

        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Length::get is not implemented");
            }
        }

        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Position::get is not implemented");
            }

            void set(LONGLONG pos) override
            {
                throw gcnew InvalidOperationException("Position::set is not implemented");
            }
        }

        virtual void Flush() override
        {
            throw gcnew InvalidOperationException("Flush is not implemented");
        }

        virtual LONGLONG Seek(LONGLONG pos, System::IO::SeekOrigin origin) override
        {
            throw gcnew InvalidOperationException("Seek is not implemented");
        }

        virtual void SetLength(LONGLONG length) override
        {
            throw gcnew InvalidOperationException("SetLength is not implemented");
        }

        virtual int Read(array<unsigned char>^ buffer, int offset, int count) override
        {
            throw gcnew InvalidOperationException("Read is not implemented");
        }

        virtual void Write(array<unsigned char>^ buffer, int offset, int count) override
        {
            throw gcnew InvalidOperationException("Write is not implemented");
        }

        virtual void Close() override
        {
            throw gcnew InvalidOperationException("Close is not implemented");
        }
    };

    //
    template<typename Input>
    ref class ScopeStreamWrapper<BinaryInputStreamBase<Input>> : public System::IO::Stream
    {
        BinaryInputStreamBase<Input> * m_baseStream;
        
    public:
        ScopeStreamWrapper(BinaryInputStreamBase<Input> * baseStream) : m_baseStream(baseStream)
        {
        }

        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                return true;
            }
        }

        virtual property bool CanSeek
        {
            bool get() override
            {
                return false;
            }
        }

        virtual property bool CanWrite
        {
            bool get() override
            {
                return false;
            }
        }

        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Length::get is not implemented");
            }
        }

        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Position::get is not implemented");
            }

            void set(LONGLONG) override
            {
                throw gcnew InvalidOperationException("Position::set is not implemented");
            }
        }

        virtual void Flush() override
        {
            throw gcnew InvalidOperationException("Flush is not implemented");
        }

        virtual LONGLONG Seek(LONGLONG, System::IO::SeekOrigin) override
        {
            throw gcnew InvalidOperationException("Seek is not implemented");
        }

        virtual void SetLength(LONGLONG) override
        {
            throw gcnew InvalidOperationException("SetLength is not implemented");
        }

        virtual int Read(array<unsigned char>^ buffer, int offset, int count) override
        {
            try
            {
                pin_ptr<unsigned char> bufdst = &buffer[0];
                return m_baseStream->Read((char*)(bufdst+offset), count);
            }
            catch (std::exception &e)
            {
                throw ScopeNativeExceptionHelper::WrapNativeException(e);
            }
        }

        virtual void Write(array<unsigned char>^, int, int) override
        {
            throw gcnew InvalidOperationException("Write is not implemented");
        }

        virtual void Close() override
        {
        }
    };

    // Managed wrapper for binary output streams
    template<typename Output>
    ref class ScopeStreamWrapper<BinaryOutputStreamBase<Output>> : public System::IO::Stream
    {
        BinaryOutputStreamBase<Output> * m_baseStream;
        
    public:
        ScopeStreamWrapper(BinaryOutputStreamBase<Output> * baseStream) : m_baseStream(baseStream)
        {
        }

        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                return false;
            }
        }

        virtual property bool CanSeek
        {
            bool get() override
            {
                return false;
            }
        }

        virtual property bool CanWrite
        {
            bool get() override
            {
                return true;
            }
        }

        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Length is supported only for write stream");
            }
        }

        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Position::get is not implemented");
            }

            void set(LONGLONG) override
            {
                throw gcnew InvalidOperationException("Position::set is not implemented");
            }
        }

        virtual void Flush() override
        {
        }

        virtual LONGLONG Seek(LONGLONG, System::IO::SeekOrigin) override
        {
            throw gcnew InvalidOperationException("Seek is not implemented");
        }

        virtual void SetLength(LONGLONG) override
        {
            throw gcnew InvalidOperationException("SetLength is not implemented");
        }

        virtual int Read(array<unsigned char>^, int, int) override
        {
            throw gcnew InvalidOperationException("Read is not implemented");
        }

        virtual void Write(array<unsigned char>^ buffer, int offset, int count) override
        {
            try
            {
                pin_ptr<unsigned char> bufdst = &buffer[0];
                return m_baseStream->Write((char*)(bufdst+offset), count);
            }   
            catch (std::exception &e)
            {
                throw ScopeNativeExceptionHelper::WrapNativeException(e);
            }
        }

        virtual void Close() override
        {
        }
    };

    // Managed wrapper for text output streams
    template<>
    ref class ScopeStreamWrapper<TextOutputStreamBase> : public System::IO::Stream
    {
        TextOutputStreamBase * m_baseStream;
        
    public:
        ScopeStreamWrapper(TextOutputStreamBase * baseStream) : m_baseStream(baseStream)
        {
        }
    
        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                return false;
            }
        }
    
        virtual property bool CanSeek
        {
            bool get() override
            {
                return false;
            }
        }
    
        virtual property bool CanWrite
        {
            bool get() override
            {
                return true;
            }
        }
    
        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Length is supported only for write stream");
            }
        }
    
        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Position::get is not implemented");
            }
    
            void set(LONGLONG /*pos*/) override
            {
                throw gcnew InvalidOperationException("Position::set is not implemented");
            }
        }
    
        virtual void Flush() override
        {
        }
    
        virtual LONGLONG Seek(LONGLONG, System::IO::SeekOrigin) override
        {
            throw gcnew InvalidOperationException("Seek is not implemented");
        }
    
        virtual void SetLength(LONGLONG) override
        {
            throw gcnew InvalidOperationException("SetLength is not implemented");
        }
    
        virtual int Read(array<unsigned char>^, int, int) override
        {
            throw gcnew InvalidOperationException("Read is not implemented");
        }
    
        virtual void Write(array<unsigned char>^ buffer, int offset, int count) override
        {
            try
            {
                pin_ptr<unsigned char> bufdst = &buffer[0];
                return m_baseStream->WriteToOutput((char*)(bufdst+offset), count);
            }            
            catch (std::exception &e)
            {
                throw ScopeNativeExceptionHelper::WrapNativeException(e);
            }
        }

        virtual void Close() override
        {
        }
    };


    // Managed wrapper for sstream data output streams
    template<>
    ref class ScopeStreamWrapper<SStreamDataOutputStream> : public System::IO::Stream
    {
        SStreamDataOutputStream * m_baseStream;
        
    public:
        ScopeStreamWrapper(SStreamDataOutputStream * baseStream) : m_baseStream(baseStream)
        {
        }
    
        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                return false;
            }
        }
    
        virtual property bool CanSeek
        {
            bool get() override
            {
                return false;
            }
        }
    
        virtual property bool CanWrite
        {
            bool get() override
            {
                return true;
            }
        }
    
        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Length is supported only for write stream");
            }
        }
    
        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                return m_baseStream->GetPosition();
            }
    
            void set(LONGLONG /*pos*/) override
            {
                throw gcnew InvalidOperationException("Position::set is not implemented");
            }
        }
    
        virtual void Flush() override
        {
        }
    
        virtual LONGLONG Seek(LONGLONG, System::IO::SeekOrigin) override
        {
            throw gcnew InvalidOperationException("Seek is not implemented");
        }
    
        virtual void SetLength(LONGLONG) override
        {
            throw gcnew InvalidOperationException("SetLength is not implemented");
        }
    
        virtual int Read(array<unsigned char>^, int, int) override
        {
            throw gcnew InvalidOperationException("Read is not implemented");
        }
    
        virtual void Write(array<unsigned char>^ buffer, int offset, int count) override
        {
            try
            {
                pin_ptr<unsigned char> bufdst = &buffer[0];
                return m_baseStream->Write((char*)(bufdst+offset), count);
            }
            catch (std::exception &e)
            {
                throw ScopeNativeExceptionHelper::WrapNativeException(e);
            }
        }

        virtual void Close() override
        {
        }
    };

    static INLINE void DeserializeUDT(ScopeUDTColumnType & s, System::IO::BinaryReader^ binaryReader)
    {
        try
        {
            ScopeUDTColumnTypeHelper::Deserialize(s, binaryReader);
        }
        catch(System::IO::EndOfStreamException^)
        {
            //Translate EndOfStreamException exception to ScopeStreamException.
            throw ScopeStreamException(ScopeStreamException::EndOfFile);
        }
    }

    extern INLINE void DeserializeUDT(ScopeUDTColumnType & s, BinaryInputStreamBase<CosmosInput> * baseStream)
    {
        // Lazily construct managed wrapper
        if (!baseStream->Wrapper())
        {
            baseStream->Wrapper().reset(new ScopeManagedHandle(gcnew System::IO::BinaryReader(gcnew ScopeStreamWrapper<BinaryInputStreamBase<CosmosInput>>(baseStream))));
        }

        DeserializeUDT(s, scope_handle_cast<System::IO::BinaryReader^>(*baseStream->Wrapper()));
    }

    extern INLINE void DeserializeUDT(ScopeUDTColumnType & s, BinaryInputStreamBase<MemoryInput> * baseStream)
    {
        // Lazily construct managed wrapper
        if (!baseStream->Wrapper())
        {
            baseStream->Wrapper().reset(new ScopeManagedHandle(gcnew System::IO::BinaryReader(gcnew ScopeStreamWrapper<BinaryInputStreamBase<MemoryInput>>(baseStream))));
        }

        DeserializeUDT(s, scope_handle_cast<System::IO::BinaryReader^>(*baseStream->Wrapper()));
    }

    extern INLINE void SSLibDeserializeUDT(ScopeUDTColumnType & s, BYTE* buffer, int offset, int length, ScopeSStreamSchema& schema)
    {
        array<Byte>^ byteArray = gcnew array<Byte>(length + 2);
        System::Runtime::InteropServices::Marshal::Copy((IntPtr)buffer,byteArray, offset, length);
        ScopeUDTColumnTypeHelper::SSLibDeserializeUDT(s, byteArray, offset, length, schema);
    }

    // Serialize UDT into the BinaryOutputStream
    extern INLINE void SerializeUDT(const ScopeUDTColumnType & s, BinaryOutputStreamBase<CosmosOutput> * baseStream)
    {
        // Lazily construct managed wrapper
        if (!baseStream->Wrapper())
        {
            baseStream->Wrapper().reset(new ScopeManagedHandle(gcnew System::IO::BinaryWriter(gcnew ScopeStreamWrapper<BinaryOutputStreamBase<CosmosOutput>>(baseStream))));
        }

        System::IO::BinaryWriter^ bw = scope_handle_cast<System::IO::BinaryWriter^>(*baseStream->Wrapper());
        ScopeUDTColumnTypeHelper::Serialize(s, bw);
        bw->Flush();
    }

    // Serialize UDT into MemoryOutputStream, 
    extern INLINE void SerializeUDT(const ScopeUDTColumnType & s, BinaryOutputStreamBase<MemoryOutput>* baseStream)
    {
        // Lazily construct managed wrapper
        if (!baseStream->Wrapper())
        {
            baseStream->Wrapper().reset(new ScopeManagedHandle(gcnew System::IO::BinaryWriter(gcnew ScopeStreamWrapper<BinaryOutputStreamBase<MemoryOutput>>(baseStream))));
        }

        System::IO::BinaryWriter^ bw = scope_handle_cast<System::IO::BinaryWriter^>(*baseStream->Wrapper());
        ScopeUDTColumnTypeHelper::Serialize(s, bw);
        bw->Flush();
    }
    
    
    // Serialize UDT into the TextOutputStream
    extern INLINE void SerializeUDT(const ScopeUDTColumnType & s, TextOutputStreamBase * baseStream)
    {
        // Lazily construct managed wrapper
        if (!baseStream->Wrapper())
        {
            baseStream->Wrapper().reset(new ScopeManagedHandle(gcnew System::IO::StreamWriter(gcnew ScopeStreamWrapper<TextOutputStreamBase>(baseStream))));
        }

        System::IO::StreamWriter^ sw = scope_handle_cast<System::IO::StreamWriter^>(*baseStream->Wrapper());
        ScopeUDTColumnTypeHelper::Serialize(s, sw);
        sw->Flush();
    }

    extern INLINE void SerializeUDT(const ScopeUDTColumnType & s, SStreamDataOutputStream * baseStream)
    {
        // Lazily construct managed wrapper
        if (!baseStream->Wrapper())
        {
            baseStream->Wrapper().reset(new ScopeManagedHandle(gcnew System::IO::BinaryWriter(gcnew ScopeStreamWrapper<SStreamDataOutputStream>(baseStream))));
        }

        System::IO::BinaryWriter^ bw = scope_handle_cast<System::IO::BinaryWriter^>(*baseStream->Wrapper());
        ScopeUDTColumnTypeHelper::Serialize(s, bw);
        bw->Flush();
    }

    extern INLINE void SSLibSerializeUDT(const ScopeUDTColumnType & s, SStreamDataOutputStream * baseStream)
    {
        // Lazily construct managed wrapper
        if (!baseStream->Wrapper())
        {
            baseStream->Wrapper().reset(new ScopeManagedHandle(gcnew ScopeStreamWrapper<SStreamDataOutputStream>(baseStream)));
        }

        System::IO::Stream^ stream = scope_handle_cast<System::IO::Stream^>(*baseStream->Wrapper());
        ScopeUDTColumnTypeHelper::SSLibSerializeUDT(s, stream);
        stream->Flush();
    }

    INLINE array<String^>^ ConvertArgsToArray(const std::wstring& wargs)
    {
        String^ args = gcnew String(wargs.c_str());
        array<String^>^ arr_args = args->Length > 0 ? args->Split(gcnew array<wchar_t>(2){L' ', L'\t'}) : gcnew array<String^>(0){};
        for(int i=0; i<arr_args->Length; ++i)
        {
            arr_args[i] = ScopeRuntime::EscapeStrings::UnEscape(arr_args[i]);
        }

        return arr_args;
    }

    INLINE void SendMemoryLoadNotification(ScopeRuntime::UDOBase^ rowset)
    {
/* TODO investigate performance impact
        unsigned long loadPercent;
        unsigned __int64 availableBytes;
        MemoryStatusResult result = MemoryManager::GetGlobal()->GetMemoryLoadStat(loadPercent, availableBytes);
        if (result == MSR_Failure)
            return;

        if (result == MSR_NoClrHosting)
        {
            unsigned __int64 maxClrMemoryUsageSize = 5ULL * 1024 * 1024 *1024; // 5GB
            unsigned __int64 currentMemorySize = GC::GetTotalMemory(false);
            if (maxClrMemoryUsageSize <= currentMemorySize)
            {
                availableBytes = 0;
                loadPercent = 100;
            }
            else
            {
                availableBytes = maxClrMemoryUsageSize - currentMemorySize;
                loadPercent = (unsigned long)((double)currentMemorySize / maxClrMemoryUsageSize * 100);
            }
        }
        rowset->MemoryLoadNotification(loadPercent, availableBytes);
*/
    }

    //
    // ScopeRowset is a managed class implementing ScopeRuntime.RowSet interface.
    // It can be used in any place where an input RowSet is expected (i.e. Processors, Reducers, Combiners).
    //
    // Input parametes:
    //        GetNextRow function - native function what generates a new row (i.e. pointer to some Operator::GetNextRow)
    //        A native row - passed to the GetNextRow
    //        Schema name - used to construct a managed row
    //
    ref class ScopeRowset abstract : public ScopeRuntime::RowSet, public Generic::IEnumerable<ScopeRuntime::Row^>
    {
        ref class ScopeRowsetEnumerator : public Generic::IEnumerator<ScopeRuntime::Row^>
        {
            bool                      m_hasRow;
            ScopeRowset^              m_inputRowset;
            unsigned __int64          m_rowCount;

        public:
            ScopeRowsetEnumerator(ScopeRowset^ inputRowset) :
                m_inputRowset(inputRowset), 
                m_hasRow(false),
                m_rowCount(0)
            {
            }

            ~ScopeRowsetEnumerator()
            {
            }

            property unsigned __int64 RowCount
            {
                unsigned __int64 get()
                {
                    return m_rowCount;
                }
            }

            property ScopeRuntime::Row^ Current
            {
                virtual ScopeRuntime::Row^ get()
                {
                    return m_hasRow ? m_inputRowset->DefaultRow : nullptr;
                }
            }

            property Object^ CurrentBase
            {
                virtual Object^ get() sealed = IEnumerator::Current::get
                {
                    return Current;
                }
            }
       
            virtual bool MoveNext()
            {
                try
                {
                    if ((m_hasRow = m_inputRowset->MoveNext()) == true)
                    {
                        m_rowCount++;
                    }

                    return m_hasRow;
                }
                catch (std::exception& ex)
                {
                    throw ScopeNativeExceptionHelper::WrapNativeException(ex);
                }
            } 
        
            virtual void Reset()
            {
                throw gcnew NotImplementedException();
            }
        };

        ScopeRowsetEnumerator^ m_enumeratorOutstanding;

    protected:
        //
        // ScopeKeySet which is used for reducer needs distinct enumerator for each key range
        //
        void ResetEnumerator()
        {
            m_enumeratorOutstanding = nullptr;
        }

    public:
        ScopeRowset(ScopeRuntime::Row^ outputrow) : m_enumeratorOutstanding(nullptr)
        {
            // inherited from RowSet class
            _outputRow = outputrow;
            _outputSchema = _outputRow->Schema;
        }

        //
        // ScopeRuntime.RowSet part
        //
        virtual property Generic::IEnumerable<ScopeRuntime::Row^>^ Rows 
        {
            Generic::IEnumerable<ScopeRuntime::Row^>^ get() override
            {
                return this;
            }
        }

        //
        // Amount of rows processed by the iterator
        //
        property unsigned __int64 RowCount
        {
            unsigned __int64 get()
            {
                if (m_enumeratorOutstanding != nullptr)
                {
                    return m_enumeratorOutstanding->RowCount;
                }

                return 0;
            }
        }

        //
        // IEnumerable part
        //
        virtual Generic::IEnumerator<ScopeRuntime::Row^>^ GetEnumerator() sealed = Generic::IEnumerable<ScopeRuntime::Row^>::GetEnumerator
        {
            if (m_enumeratorOutstanding != nullptr)
            {
                throw gcnew InvalidOperationException("User Error: Multiple instances of enumerators are not supported on input RowSet.Rows. The input Rowset.Rows may be enumerated only once. If user code needs to enumerate it multiple times, then all Rows must be cached during the first pass and use cached Rows later.");
            }

            m_enumeratorOutstanding = gcnew ScopeRowsetEnumerator(this);

            // Do not return "this" to avoid object distruction by Dispose()
            return m_enumeratorOutstanding;
        }

        virtual IEnumerator^ GetEnumeratorBase() sealed = IEnumerable::GetEnumerator
        {
            return GetEnumerator();
        }

        //
        // IEnumerator part (to be defined in derived class)
        //
        virtual bool MoveNext() abstract;
    };

    //
    // Enumerate rowset in physical order
    //
    template <typename InputSchema>
    ref class ScopeInputRowset : public ScopeRowset
    {
        bool m_hasMoreRows;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallType)(InputSchema&, ScopeRuntime::Row^%, InteropAllocator * alloc);

        InputSchema& m_nativeRow;
        OperatorDelegate<InputSchema> * m_child;
        MarshalToManagedCallType m_marshalToManaged;
        InteropAllocator * m_allocator;
        ScopeRuntime::UDOBase^ m_parentRowset;

    public:
        ScopeInputRowset(InputSchema& nativeRow, OperatorDelegate<InputSchema> * child, ScopeRuntime::UDOBase^ parentRowset, MarshalToManagedCallType marshalToManagedCall) 
            : ScopeRowset(ManagedRow<InputSchema>().get()),
              m_hasMoreRows(true),
              m_nativeRow(nativeRow),
              m_child(child),
              m_parentRowset(parentRowset),
              m_marshalToManaged(marshalToManagedCall)
        {
            m_allocator = new InteropAllocator(_outputSchema->Count, "ScopeInputRowset");
        }

        ~ScopeInputRowset()
        {
            this->!ScopeInputRowset();
        }

        !ScopeInputRowset()
        {
            delete m_allocator;
            m_allocator = nullptr;
        }

        virtual bool MoveNext() override
        {
            if (m_hasMoreRows)
            {
                m_hasMoreRows = m_child->GetNextRow(m_nativeRow);
                SendMemoryLoadNotification(m_parentRowset);
            }

            if (!m_hasMoreRows)
            {
                return false;
            }

            (*m_marshalToManaged)(m_nativeRow, _outputRow, m_allocator);

            return true;
        }

        void MarshalToManagedRow()
        {
            (*m_marshalToManaged)(m_nativeRow, _outputRow, m_allocator);
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("ScopeInputRowset");
            if (m_allocator != nullptr)
            {
                m_allocator->WriteRuntimeStats(node);
            }
            m_child->WriteRuntimeStats(node);
        }

        InputSchema& GetNativeRow()
        {
            SCOPE_ASSERT(m_hasMoreRows);
            return m_nativeRow;
        }

        ScopeRuntime::Row^ GetManagedRow()
        {
            SCOPE_ASSERT(m_hasMoreRows);
            return _outputRow;
        }
    };

    //
    // Enumerate rowset honoring key ranges
    //
    template <typename InputSchema, typename KeyPolicy>
    ref class ScopeInputKeyset : public ScopeRowset
    {
        enum class State { eINITIAL, eSTART, eRANGE, eEND, eEOF };

        State m_state;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallType)(InputSchema&, typename KeyPolicy::KeyType&, ScopeRuntime::Row^%, InteropAllocator*);

        InputSchema&                                            m_nativeRow;
        MarshalToManagedCallType                                m_marshalToManaged;
        InteropAllocator *                                      m_allocator;
        KeyIterator<OperatorDelegate<InputSchema>, KeyPolicy> * m_iter;
        ScopeRuntime::UDOBase^ m_parentRowset;

    public:
        ScopeInputKeyset(InputSchema& nativeRow, OperatorDelegate<InputSchema> * child, ScopeRuntime::UDOBase^ parentRowset, MarshalToManagedCallType marshalToManagedCall) 
            : ScopeRowset(ManagedRow<InputSchema>().get()),
              m_state(State::eINITIAL),
              m_nativeRow(nativeRow),
              m_parentRowset(parentRowset),
              m_marshalToManaged(marshalToManagedCall)
        {
            m_allocator = new InteropAllocator(_outputSchema->Count, "ScopeInputKeyset");
            m_iter = new KeyIterator<OperatorDelegate<InputSchema>, KeyPolicy>(child);
        }

        ~ScopeInputKeyset()
        {
            this->!ScopeInputKeyset();
        }

        !ScopeInputKeyset()
        {
            delete m_allocator;
            m_allocator = nullptr;

            delete m_iter;
            m_iter = nullptr;
        }

        bool Init()
        {
            SCOPE_ASSERT(m_state == State::eINITIAL);

            m_iter->ReadFirst();
            m_iter->ResetKey();

            m_state = m_iter->End() ? State::eEOF : State::eSTART;

            return m_state == State::eSTART;
        }

        //
        // End current key range and start next one
        //
        bool NextKey()
        {
            SCOPE_ASSERT(m_state != State::eINITIAL);

            switch(m_state)
            {
            case State::eSTART:
            case State::eRANGE:
                m_iter->Drain();
                // Fallthrough
            case State::eEND:
                m_iter->ResetKey();
                ResetEnumerator();
                m_state = m_iter->End() ? State::eEOF : State::eSTART;
            }

            return m_state == State::eSTART;
        }

        virtual bool MoveNext() override
        {
            SCOPE_ASSERT(m_state != State::eINITIAL);

            switch(m_state)
            {
            case State::eSTART:
                // Row was already read
                m_state = State::eRANGE;
                break;

            case State::eRANGE:
                m_iter->Increment();
                m_state = m_iter->End() ? State::eEND : State::eRANGE;
                break;
            }

            if (m_state != State::eRANGE)
            {
                return false;
            }

            m_nativeRow = *m_iter->GetRow();

            SendMemoryLoadNotification(m_parentRowset);

            (*m_marshalToManaged)(m_nativeRow, *m_iter->GetKey(), _outputRow, m_allocator);

            return true;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("ScopeInputKeyset");
            if (m_allocator != nullptr)
            {
                m_allocator->WriteRuntimeStats(node);
            }
            if (m_iter != nullptr)
            {
                m_iter->WriteRuntimeStats(node);
            }
        }
    };

#pragma region ManagedOperators
    template<typename OutputSchema, int RunScopeCEPMode = SCOPECEP_MODE_NONE>
    class ScopeExtractorManagedImpl : public ScopeExtractorManaged<OutputSchema>
    {
        friend struct ScopeExtractorManagedFactory;

        static const char* const sm_className;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^> m_enumerator;
        ScopeTypedManagedHandle<ScopeRuntime::Extractor ^>                  m_extractor;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        ScopeTypedManagedHandle<System::IO::StreamReader ^>                 m_reader;
        ScopeTypedManagedHandle<ScopeCosmosStream ^>                        m_inputStream;
        MarshalToNativeCallType                                             m_marshalToNative;
        IncrementalAllocator                                                m_allocator;

        ScopeExtractorManagedImpl(ScopeRuntime::Extractor ^ udo, Generic::List<String^>^ args, MarshalToNativeCallType marshalCall):
            m_extractor(udo),
            m_args(args),
            m_marshalToNative(marshalCall),
            m_allocator()
        {
        }

    public:
        virtual void CreateInstance(const InputFileInfo& input, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize)
        {
            m_inputStream = gcnew ScopeCosmosStream(input.inputFileName, true, bufSize, bufCount);
            if (RunScopeCEPMode == SCOPECEP_MODE_NONE) 
            {
                m_reader = gcnew System::IO::StreamReader(m_inputStream.get());
            }
            else
            {
                m_reader = gcnew ScopeRuntime::StreamReaderWithPosition(m_inputStream.get());
            }

            m_allocator.Init(virtualMemSize, sm_className);
        }

        virtual void Init()
        {
            m_inputStream->Init();
            m_enumerator = m_extractor->Extract(m_reader.get(), m_extractor->DefaultRow, m_args->ToArray())->GetEnumerator();
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

            if (enumerator->MoveNext())
            {
                if (enumerator->Current == nullptr || !Object::ReferenceEquals(m_extractor->DefaultRow->GetType(), enumerator->Current->GetType()))
                {
                    throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                }

                m_allocator.Reset();
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                SendMemoryLoadNotification(m_extractor);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            m_reader->Close();
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();

            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputStream)
            {
                m_inputStream->WriteRuntimeStats(node);
            }
        }

        virtual ~ScopeExtractorManagedImpl()
        {
            // There only to ensure proper destruction when base class destructor is called
            if (m_inputStream)
            {
                try
                {
                    m_inputStream.reset();
                }
                catch (std::exception&)
                {
                    // ignore any I/O errors as we are destroying the object anyway
                }
            }
        }

        // Time spend in reading
        virtual __int64 GetIOTime()
        {
            if (m_inputStream)
            {
                return m_inputStream->GetInclusiveTimeMillisecond();
            }

            return 0;
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            System::IO::BinaryWriter^ checkpoint = gcnew System::IO::BinaryWriter(gcnew ScopeCosmosStream(&output.GetOutputer(), 0x400000, 2));
            m_extractor->DoStreamingCheckpoint(checkpoint);
            checkpoint->Write((int)SCOPECEP_CHECKPINT_MAGICNUMBER);
            checkpoint->Flush();
            unsigned __int64 position = static_cast<ScopeRuntime::StreamReaderWithPosition^>(m_reader.get())->Position;
            if (position == 0)
            {
                position = m_inputStream.get()->Position;
            }
            m_inputStream->SaveState(output, position);

        }
        
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            System::IO::BinaryReader^ checkpoint = gcnew System::IO::BinaryReader(gcnew ScopeCosmosStream(&input.GetInputer(), 0x400000, 2));
            m_extractor->LoadStreamingCheckpoint(checkpoint);
            CheckScopeCEPCheckpointMagicNumber(checkpoint);
            m_inputStream->LoadState(input);
        }
    };

    template<typename OutputSchema, int RunScopeCEPMode>
    const char* const ScopeExtractorManagedImpl<OutputSchema, RunScopeCEPMode>::sm_className = "ScopeExtractorManagedImpl";

    template<typename OutputSchema, int UID, int RunScopeCEPMode>
    INLINE ScopeExtractorManaged<OutputSchema> * ScopeExtractorManagedFactory::Make(std::string * argv, int argc)
    {
        ManagedUDO<UID> managedUDO(argv, argc);
        return new ScopeExtractorManagedImpl<OutputSchema, RunScopeCEPMode>(managedUDO.get(),
                                                           managedUDO.args(),
                                                           &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal);
    }

    template<typename OutputSchema>
    class ScopeSStreamExtractorManagedImpl : public ScopeSStreamExtractorManaged<OutputSchema>
    {
        friend struct ScopeSStreamExtractorManagedFactory;
        static const char* const sm_className;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<System::Collections::Generic::List<ScopeCosmosStream^>^> m_streams;
        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^>              m_enumerator;
        ScopeTypedManagedHandle<ScopeRuntime::SStreamExtractor ^>                        m_extractor;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                                m_args;
        MarshalToNativeCallType                                                          m_marshalToNative;
        IncrementalAllocator                                                             m_allocator;

        ScopeSStreamExtractorManagedImpl(ScopeRuntime::SStreamExtractor ^ udo, Generic::List<String^>^ args, MarshalToNativeCallType marshalCall):
            m_streams(gcnew System::Collections::Generic::List<ScopeCosmosStream^>()),
            m_extractor(udo),
            m_args(args),
            m_marshalToNative(marshalCall),
            m_allocator()
        {
        }

    public:
        virtual void Init(const int ssid, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize)
        {
            auto devices = IOManager::GetGlobal()->GetSStreamDevices(ssid);
            auto processingGroupIds = IOManager::GetGlobal()->GetSStreamProcessingGroupIds(ssid);
            SCOPE_ASSERT(devices.size() == processingGroupIds.size());

            auto processingGroups = gcnew System::Collections::Generic::SortedList<int, System::Collections::Generic::List<System::Collections::Generic::List<System::IO::Stream^>^>^>();
            for (int i = 0; i < devices.size(); i++)
            {
                auto stream = gcnew ManagedSSLibScopeCosmosStream(devices[i], true, bufSize, bufCount);
                m_streams->Add(stream);
                auto streamList =  gcnew System::Collections::Generic::List<System::IO::Stream^>();
                streamList->Add(StructuredStream::SSLibHelper::GetSyncStream(stream));
                if (processingGroups->Count == 0 || processingGroupIds[i] != processingGroupIds[i - 1])
                {
                    auto processingGroup = gcnew System::Collections::Generic::List<System::Collections::Generic::List<System::IO::Stream^>^>();
                    processingGroup->Add(streamList);
                    processingGroups->Add(processingGroupIds[i], processingGroup);
                }
                else
                {
                    processingGroups[processingGroupIds[i]]->Add(streamList);
                }
            }

            m_extractor->Initialize(processingGroups, m_extractor->Schema, m_args->ToArray(), false);
            m_extractor->OpenPartitionedStreams();
            m_enumerator = m_extractor->Rows->GetEnumerator();
            m_allocator.Init(virtualMemSize, sm_className);
        }

        virtual string GetKeyRangeFileName()
        {
            if (m_extractor->KeyRangeFile == nullptr)
            {
                return "";
            }
            
            return msclr::interop::marshal_as<std::string>(m_extractor->KeyRangeFile);
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

            if (enumerator->MoveNext())
            {
                if (enumerator->Current == nullptr || !Object::ReferenceEquals(m_extractor->DefaultRow->GetType(), enumerator->Current->GetType()))
                {
                    throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                }

                m_allocator.Reset();
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            m_extractor->CleanupDataUnitReaders();
            for(int i = 0; i < m_streams->Count; i++)
            {
                m_streams.get()[i]->Close();
            }
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
        }

        // Time spend in reading
        virtual __int64 GetIOTime()
        {
            __int64 ioTime = 0;
            for(int i = 0; i < m_streams->Count; i++)
            {
                ioTime += m_streams.get()[i]->GetInclusiveTimeMillisecond();
            }

            return ioTime;
        }
    };

    template<typename OutputSchema>
    const char* const ScopeSStreamExtractorManagedImpl<OutputSchema>::sm_className = "ScopeSStreamExtractorManagedImpl";

    template<typename OutputSchema, int UID>
    INLINE ScopeSStreamExtractorManaged<OutputSchema> * ScopeSStreamExtractorManagedFactory::Make()
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        return new ScopeSStreamExtractorManagedImpl<OutputSchema>(managedUDO.get(),
                                                                  managedUDO.args(),
                                                                  &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal);
    }

    template<typename InputSchema, typename OutputSchema>
    class ScopeProcessorManagedImpl : public ScopeProcessorManaged<InputSchema, OutputSchema>
    {
        friend struct ScopeProcessorManagedFactory;

        static const char* const sm_className;

        InputSchema m_inputRow;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallType)(InputSchema &, ScopeRuntime::Row ^%, InteropAllocator *);
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);
        typedef bool (*TransformRowCallType)(InputSchema &, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^> m_enumerator;
        ScopeTypedManagedHandle<ScopeInputRowset<InputSchema> ^>            m_inputRowset;
        ScopeTypedManagedHandle<ScopeRuntime::Processor ^>                  m_processor;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        MarshalToNativeCallType                                             m_marshalToNative;
        TransformRowCallType                                                m_transformRowCall;
        RowEntityAllocator                                                  m_allocator;

        ScopeProcessorManagedImpl(ScopeRuntime::Processor ^ udo, Generic::List<String^>^ args, OperatorDelegate<InputSchema> * child, MarshalToNativeCallType marshalToNativeCall, MarshalToManagedCallType marshalToManagedCall, TransformRowCallType transformRowCall):
            m_processor(udo),
            m_args(args),
            m_marshalToNative(marshalToNativeCall),
            m_transformRowCall(transformRowCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent)
        {
            m_inputRowset = gcnew ScopeInputRowset<InputSchema>(m_inputRow, child, udo, marshalToManagedCall);
        }

    public:
        virtual void Init()
        {
            // Initialize processors (this internally calls InitializeAtRuntime)
            m_processor->Initialize(m_inputRowset.get(), m_processor->DefaultRow->Schema, m_args->ToArray());

            m_enumerator = m_processor->Process(m_inputRowset.get(), m_processor->DefaultRow, m_args->ToArray())->GetEnumerator();
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

            if (enumerator->MoveNext())
            {
                if (enumerator->Current == nullptr  || !Object::ReferenceEquals(m_processor->DefaultRow->GetType(), enumerator->Current->GetType()))
                {
                    throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                }

                m_allocator.Reset();
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);
                (*m_transformRowCall)(m_inputRow, output, nullptr);

                SendMemoryLoadNotification(m_processor);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputRowset)
            {
                m_inputRowset->WriteRuntimeStats(node);
            }
        }

        virtual ~ScopeProcessorManagedImpl()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            System::IO::BinaryWriter^ checkpoint = gcnew System::IO::BinaryWriter(gcnew ScopeCosmosStream(&output.GetOutputer(), 0x400000, 2));
            m_processor->DoStreamingCheckpoint(checkpoint);
            checkpoint->Write((int)SCOPECEP_CHECKPINT_MAGICNUMBER);
            checkpoint->Flush();
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            System::IO::BinaryReader^ checkpoint = gcnew System::IO::BinaryReader(gcnew ScopeCosmosStream(&input.GetInputer(), 0x400000, 2));
            m_processor->LoadStreamingCheckpoint(checkpoint);
            CheckScopeCEPCheckpointMagicNumber(checkpoint);
        }
    };

    template<typename InputSchema, typename OutputSchema>
    const char* const ScopeProcessorManagedImpl<InputSchema, OutputSchema>::sm_className = "ScopeProcessorManagedImpl";

    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeProcessorManagedFactory::Make(OperatorDelegate<InputSchema> * child)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        return new ScopeProcessorManagedImpl<InputSchema, OutputSchema>(managedUDO.get(),
                                                                        managedUDO.args(),
                                                                        child,
                                                                        &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal,
                                                                        &InteropToManagedRowPolicy<InputSchema,UID>::Marshal,
                                                                        &RowTransformPolicy<InputSchema,OutputSchema,UID>::FilterTransformRow);
    }

    template<typename InputOperators, typename OutputSchema, int UID>
    class MultiProcessorPolicy
    {
    public:
        explicit MultiProcessorPolicy(InputOperators* children);

        void Init();
        void Close();
        Generic::List<ScopeRuntime::RowSet^>^ GetInputRowsets();
        LONGLONG GetInclusiveTimeMillisecond();
        void WriteRuntimeStats(TreeNode& root);
        
        void DoScopeCEPCheckpoint(BinaryOutputStream & output);
        void LoadScopeCEPCheckpoint(BinaryInputStream & output);
    };

    template<typename InputOperators, typename OutputSchema, int UID>
    class ScopeMultiProcessorManagedImpl : public ScopeMultiProcessorManaged<InputOperators, OutputSchema, UID>
    {
        static const char* const sm_className;

        typedef typename MultiProcessorPolicy<InputOperators, OutputSchema, UID> MultiProccessorPolicyType;
        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^> m_enumerator;
        ScopeTypedManagedHandle<Generic::List<ScopeRuntime::RowSet^> ^>     m_inputRowsets;
        ScopeTypedManagedHandle<ScopeRuntime::MultiProcessor ^>             m_processor;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        ScopeTypedManagedHandle<System::IO::Stream^>                        m_contextInputStream;
        ScopeTypedManagedHandle<System::IO::Stream^>                        m_contextOutputStream;
        MarshalToNativeCallType                                             m_marshalToNative;
        RowEntityAllocator                                                  m_allocator;
        MultiProccessorPolicyType                                           m_processorPolicy;

    public:
        ScopeMultiProcessorManagedImpl(ScopeRuntime::MultiProcessor ^ udo, Generic::List<String^>^ args, InputOperators* children, System::IO::Stream^ contextInputStream, System::IO::Stream^ contextOutputStream, MarshalToNativeCallType marshalToNativeCall):
            m_processor(udo),
            m_args(args),
            m_contextInputStream(contextInputStream),
            m_contextOutputStream(contextOutputStream),
            m_marshalToNative(marshalToNativeCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_processorPolicy(children)
        {
            SCOPE_ASSERT(contextInputStream == nullptr && contextOutputStream == nullptr
                || contextInputStream != nullptr && contextInputStream != nullptr);
            m_inputRowsets = m_processorPolicy.GetInputRowsets(udo);
        }

        virtual void Init()
        {
            if(m_contextInputStream.get() != nullptr)
            {
                m_processor->GetType()->GetMethod("SetContextStreams", BindingFlags::NonPublic | BindingFlags::Instance)->Invoke(m_processor.get(), gcnew array<Object^>(2) {m_contextInputStream.get(), m_contextOutputStream.get()});
            }

            m_processorPolicy.Init();

            // Initialize processors (this internally calls InitializeAtRuntime)
            m_processor->Initialize(m_inputRowsets.get(), m_processor->DefaultRow->Schema, m_args->ToArray());

            if (m_processor->Context != nullptr)
            {
                bool isInitialized = (bool)m_processor->Context->GetType()->GetProperty("IsInitialized", BindingFlags::NonPublic | BindingFlags::Instance)->GetValue(m_processor->Context);
                if (!isInitialized)
                {
                    m_processor->Context->Deserialize(m_contextInputStream.get());
                    m_processor->Context->GetType()->GetProperty("IsInitialized", BindingFlags::NonPublic | BindingFlags::Instance)->SetValue(m_processor->Context, true);
                    ScopeRuntime::ScopeTrace::Status("Initialize context Object Hash {0} Value {1}", m_processor->Context->GetHashCode(), m_processor->Context->ToString());
                }
                else
                {
                    ScopeRuntime::ScopeTrace::Status("Context is already loaded! Object Hash {0} Value {1}", m_processor->Context->GetHashCode(), m_processor->Context->ToString());
                }
            }
            else
            {
                ScopeRuntime::ScopeTrace::Status("Context is null");
            }

            m_enumerator = m_processor->Process(m_inputRowsets.get(), m_processor->DefaultRow, m_args->ToArray())->GetEnumerator();
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

            if (enumerator->MoveNext())
            {
                if (enumerator->Current == nullptr  || !Object::ReferenceEquals(m_processor->DefaultRow->GetType(), enumerator->Current->GetType()))
                {
                    throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                }

                m_allocator.Reset();
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                SendMemoryLoadNotification(m_processor);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            if (m_processor->Context != nullptr)
            {
                m_contextInputStream->Close();
                m_processor->Context->Serialize(m_contextOutputStream.get());
                m_contextOutputStream->Close();
            }

            m_processorPolicy.Close();
            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual LONGLONG GetInclusiveTimeMillisecond()
        {
            return m_processorPolicy.GetInclusiveTimeMillisecond();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            m_processorPolicy.WriteRuntimeStats(root);
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            System::IO::BinaryWriter^ checkpoint = gcnew System::IO::BinaryWriter(gcnew ScopeCosmosStream(&output.GetOutputer(), 0x400000, 2));
            m_processor->DoStreamingCheckpoint(checkpoint);
            checkpoint->Write((int)SCOPECEP_CHECKPINT_MAGICNUMBER);
            checkpoint->Flush();
            m_processorPolicy.DoScopeCEPCheckpoint(output);
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            System::IO::BinaryReader^ checkpoint = gcnew System::IO::BinaryReader(gcnew ScopeCosmosStream(&input.GetInputer(), 0x400000, 2));
            m_processor->LoadStreamingCheckpoint(checkpoint);
            CheckScopeCEPCheckpointMagicNumber(checkpoint);
            m_processorPolicy.LoadScopeCEPCheckpoint(input);
        }
    };

    template<typename InputOperators, typename OutputSchema, int UID>
    const char* const ScopeMultiProcessorManagedImpl<InputOperators, OutputSchema, UID>::sm_className = "ScopeMultiProcessorManagedImpl";

    template<typename InputOperators, typename OutputSchema, int UID>
    INLINE ScopeMultiProcessorManaged<InputOperators, OutputSchema, UID>* ScopeMultiProcessorManagedFactory::Make(
            InputOperators* children,
            string* inputContextFile,
            string* outputContextFile,
            SIZE_T inputBufSize,
            int inputBufCnt,
            SIZE_T outputBufSize,
            int outputBufCnt)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        ScopeCosmosStream^ contextInputStream = nullptr;
        ScopeCosmosStream^ contextOutputStream = nullptr;
        if (inputContextFile != nullptr && !inputContextFile->empty())
        {
            SCOPE_ASSERT(outputContextFile != nullptr && !outputContextFile->empty());
            contextInputStream = gcnew ScopeCosmosStream(*inputContextFile, true, inputBufSize, inputBufCnt);
            contextOutputStream = gcnew ScopeCosmosStream(*outputContextFile, false, outputBufSize, outputBufCnt, false);
            contextInputStream->Init();
            contextOutputStream->Init();
        }

        return new ScopeMultiProcessorManagedImpl<InputOperators, OutputSchema, UID>(managedUDO.get(),
                                                                                     managedUDO.args(),
                                                                                     children,
                                                                                     contextInputStream,
                                                                                     contextOutputStream,
                                                                                     &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal);
    }

    template<typename InputSchema>
    class ScopeCreateContextManagedImpl : public ScopeCreateContextManaged<InputSchema>
    {
        friend struct ScopeCreateContextManagedFactory;
        static const char* const sm_className;

        InputSchema m_inputRow;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallType) (InputSchema &, ScopeRuntime::Row ^%, InteropAllocator *);

        ScopeTypedManagedHandle<ScopeInputRowset<InputSchema> ^>  m_inputRowset;
        ScopeTypedManagedHandle<ScopeRuntime::ExecutionContext ^> m_exeContext;
        ScopeTypedManagedHandle<Generic::List<String^> ^>         m_args;
        ScopeTypedManagedHandle<ScopeCosmosStream ^>              m_outputStream;

        ScopeCreateContextManagedImpl(ScopeRuntime::ExecutionContext ^ udo, Generic::List<String^>^ args, OperatorDelegate<InputSchema> * child, MarshalToManagedCallType marshalToManagedCall):
            m_exeContext(udo),
            m_args(args)
        {
            
            m_inputRowset = gcnew ScopeInputRowset<InputSchema>(m_inputRow, child, udo, marshalToManagedCall);
        }

    public:
        virtual void Init(std::string& outputName, SIZE_T bufSize, int bufCnt)
        {
            m_outputStream = gcnew ScopeCosmosStream(outputName, false, bufSize, bufCnt, false);
            m_outputStream->Init();
            m_exeContext->GetType()->GetMethod("InitializeAtRuntime", BindingFlags::NonPublic | BindingFlags::Instance)->Invoke(
                m_exeContext.get(), gcnew array<Object^>(5) {m_inputRowset.get(), nullptr, m_outputStream.get(), m_args->ToArray(), m_exeContext->Schema});
        }

        virtual void Serialize()
        {
            m_exeContext->GetType()->GetMethod("DoIninitialize", BindingFlags::NonPublic | BindingFlags::Instance)->Invoke(
                m_exeContext.get(), gcnew array<Object^>(0));
        }

        virtual void Close()
        {
            m_outputStream->Close();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            RuntimeStats::WriteRowCount(root, (LONGLONG)m_inputRowset->RowCount);

            auto & node = root.AddElement(sm_className);

            if (m_inputRowset)
            {
                m_inputRowset->WriteRuntimeStats(node);
            }

            if (m_outputStream)
            {
                m_outputStream->WriteRuntimeStats(node);
            }
        }

        // Time in writing data
        virtual __int64 GetIOTime()
        {
            if (m_outputStream)
            {
                return m_outputStream->GetInclusiveTimeMillisecond();
            }

            return 0;
        }
    };

    template<typename InputSchema>
    const char* const ScopeCreateContextManagedImpl<InputSchema>::sm_className = "ScopeCreateContextManagedImpl";


    template<typename InputSchema, int UID>
    INLINE ScopeCreateContextManaged<InputSchema> * ScopeCreateContextManagedFactory::Make(OperatorDelegate<InputSchema> * child)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        return new ScopeCreateContextManagedImpl<InputSchema>(managedUDO.get(), 
                                                         managedUDO.args(), 
                                                         child, 
                                                         &InteropToManagedRowPolicy<InputSchema,UID>::Marshal);
    }

    template<typename OutputSchema>
    class ScopeReadContextManagedImpl : public ScopeReadContextManaged<OutputSchema>
    {
        friend struct ScopeReadContextManagedFactory;

        static const char* const sm_className;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^> m_enumerator;
        ScopeTypedManagedHandle<ScopeRuntime::ExecutionContext ^>           m_exeContext;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        ScopeTypedManagedHandle<ScopeCosmosStream ^>                        m_inputStream;
        MarshalToNativeCallType                                             m_marshalToNative;
        IncrementalAllocator                                                m_allocator;

        ScopeReadContextManagedImpl(ScopeRuntime::ExecutionContext ^ udo, Generic::List<String^>^ args, MarshalToNativeCallType marshalCall):
            m_exeContext(udo),
            m_args(args),
            m_marshalToNative(marshalCall),
            m_allocator()
        {
        }

    public:
        virtual void Init(std::string& inputName, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize)
        {
            m_inputStream = gcnew ScopeCosmosStream(inputName, true, bufSize, bufCount);
            m_inputStream->Init();

            m_exeContext->GetType()->GetMethod("InitializeAtRuntime", BindingFlags::NonPublic | BindingFlags::Instance)->Invoke(
                m_exeContext.get(), gcnew array<Object^>(5) {nullptr, m_inputStream.get(), nullptr, m_args->ToArray(), m_exeContext->Schema});
            m_enumerator = m_exeContext->Rows->GetEnumerator();
            m_allocator.Init(virtualMemSize, sm_className);
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

            if (enumerator->MoveNext())
            {
                if (enumerator->Current == nullptr  || !Object::ReferenceEquals(m_exeContext->DefaultRow->GetType(), enumerator->Current->GetType()))
                {
                    throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                }

                m_allocator.Reset();
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                SendMemoryLoadNotification(m_exeContext);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            m_inputStream->Close();
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputStream)
            {
                m_inputStream->WriteRuntimeStats(node);
            }
        }

        // Time spend in reading
        virtual __int64 GetIOTime()
        {
            if (m_inputStream)
            {
                return m_inputStream->GetInclusiveTimeMillisecond();
            }

            return 0;
        }
    };

    template<typename OutputSchema>
    const char* const ScopeReadContextManagedImpl<OutputSchema>::sm_className = "ScopeReadContextManagedImpl";

    template<typename OutputSchema, int UID>
    INLINE ScopeReadContextManaged<OutputSchema> * ScopeReadContextManagedFactory::Make()
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        return new ScopeReadContextManagedImpl<OutputSchema>(managedUDO.get(),
                                                               managedUDO.args(),
                                                               &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal);
    }

    template<typename InputSchema, typename OutputSchema, typename KeyPolicy>
    class ScopeReducerManagedImpl : public ScopeProcessorManaged<InputSchema, OutputSchema>
    {
        friend struct ScopeReducerManagedFactory;

        static const char* const sm_className;

        InputSchema m_inputRow;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallType)(InputSchema &, typename KeyPolicy::KeyType &, ScopeRuntime::Row ^%, InteropAllocator *);
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^> m_enumerator;
        ScopeTypedManagedHandle<ScopeInputKeyset<InputSchema,KeyPolicy> ^>  m_inputKeyset;
        ScopeTypedManagedHandle<ScopeRuntime::Reducer ^>                    m_reducer;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        MarshalToNativeCallType                                             m_marshalToNative;
        RowEntityAllocator                                                  m_allocator;
        bool                                                                m_hasMoreRows;

        ScopeReducerManagedImpl(ScopeRuntime::Reducer ^ udo, Generic::List<String^>^ args, OperatorDelegate<InputSchema> * child, MarshalToNativeCallType marshalToNativeCall, MarshalToManagedCallType marshalToManagedCall):
            m_reducer(udo),
            m_args(args),
            m_marshalToNative(marshalToNativeCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_hasMoreRows(false)
        {
            m_inputKeyset = gcnew ScopeInputKeyset<InputSchema,KeyPolicy>(m_inputRow, child, udo, marshalToManagedCall);
        }

    public:
        virtual void Init()
        {
            // Let reducer do custom initialization
            m_reducer->Initialize(m_inputKeyset->Schema, m_reducer->Schema, m_args->ToArray());

            m_hasMoreRows = m_inputKeyset->Init();

            if (m_hasMoreRows)
            {
                m_enumerator = m_reducer->Reduce(m_inputKeyset.get(), m_reducer->DefaultRow, m_args->ToArray())->GetEnumerator();
            }
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            while(m_hasMoreRows)
            {
                Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

                if (enumerator->MoveNext())
                {
                    if (enumerator->Current == nullptr  || !Object::ReferenceEquals(m_reducer->DefaultRow->GetType(), enumerator->Current->GetType()))
                    {
                        throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                    }

                    m_allocator.Reset();
                    (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                    SendMemoryLoadNotification(m_reducer);

                    return true;
                }
                else if (m_inputKeyset->NextKey())
                {
                    // Proceed to the next key and create enumerator for it
                    m_enumerator.reset(m_reducer->Reduce(m_inputKeyset.get(), m_reducer->DefaultRow, m_args->ToArray())->GetEnumerator());
                }
                else
                {
                    // EOF
                    m_hasMoreRows = false;
                }
            }

            return false;
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputKeyset)
            {
                m_inputKeyset->WriteRuntimeStats(node);
            }
        }

        virtual ~ScopeReducerManagedImpl()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            System::IO::BinaryWriter^ checkpoint = gcnew System::IO::BinaryWriter(gcnew ScopeCosmosStream(&output.GetOutputer(), 0x400000, 2));
            m_reducer->DoStreamingCheckpoint(checkpoint);
            checkpoint->Write((int)SCOPECEP_CHECKPINT_MAGICNUMBER);
            checkpoint->Flush();
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            System::IO::BinaryReader^ checkpoint = gcnew System::IO::BinaryReader(gcnew ScopeCosmosStream(&input.GetInputer(), 0x400000, 2));
            m_reducer->LoadStreamingCheckpoint(checkpoint);
            CheckScopeCEPCheckpointMagicNumber(checkpoint);
        }
    };

    template<typename InputSchema, typename OutputSchema, typename KeyPolicy>
    const char* const ScopeReducerManagedImpl<InputSchema, OutputSchema, KeyPolicy>::sm_className = "ScopeReducerManagedImpl";

    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeReducerManagedFactory::Make(OperatorDelegate<InputSchema> * child)
    {
        typedef KeyComparePolicy<InputSchema, UID> KeyPolicy;

        ManagedUDO<UID> managedUDO(nullptr, 0);
        return new ScopeReducerManagedImpl<InputSchema, OutputSchema, KeyPolicy>(managedUDO.get(),
                                                                                 managedUDO.args(),
                                                                                 child,
                                                                                 &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal,
                                                                                 &InteropToManagedRowPolicy<InputSchema,UID>::Marshal);
    }

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema>
    class ScopeCombinerManagedImpl : public ScopeCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema>
    {
        friend struct ScopeCombinerManagedFactory;

        static const char* const sm_className;

        InputSchemaLeft m_inputRowLeft;
        InputSchemaRight m_inputRowRight;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallTypeLeft) (InputSchemaLeft &, ScopeRuntime::Row ^%, InteropAllocator *);
        typedef void (*MarshalToManagedCallTypeRight) (InputSchemaRight &, ScopeRuntime::Row ^%, InteropAllocator *);
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^> m_enumerator;
        ScopeTypedManagedHandle<ScopeInputRowset<InputSchemaLeft> ^>        m_inputRowsetLeft;
        ScopeTypedManagedHandle<ScopeInputRowset<InputSchemaRight> ^>       m_inputRowsetRight;
        ScopeTypedManagedHandle<ScopeRuntime::RowSet ^>                     m_combiner;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        MarshalToNativeCallType                                             m_marshalToNative;
        RowEntityAllocator                                                  m_allocator;

        ScopeCombinerManagedImpl
        (
            ScopeRuntime::RowSet ^ udo,
            Generic::List<String^>^ args,
            OperatorDelegate<InputSchemaLeft> * leftChild,
            OperatorDelegate<InputSchemaRight> * rightChild,
            MarshalToNativeCallType marshalToNativeCall, 
            MarshalToManagedCallTypeLeft marshalToManagedCallLeft,
            MarshalToManagedCallTypeRight marshalToManagedCallRight
        ) : m_combiner(udo), m_args(args), m_marshalToNative(marshalToNativeCall), m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent)
        {
            m_inputRowsetLeft = gcnew ScopeInputRowset<InputSchemaLeft>(m_inputRowLeft, leftChild, udo, marshalToManagedCallLeft);
            m_inputRowsetRight = gcnew ScopeInputRowset<InputSchemaRight>(m_inputRowRight, rightChild, udo, marshalToManagedCallRight);
        }

    public:
        virtual void Init()
        {
            // Set the table names of the left and right schema (same as DoCombine in the old runtime)
            array<String^>^ args_arr = m_args->ToArray();
            Generic::List<String^>^ unusedArgs = gcnew Generic::List<String^>();
            for (int i = 0; i < args_arr->Length; ++i)
            {
                if (args_arr[i] == "-leftTable")
                {
                    m_inputRowsetLeft.get()->Schema->SetTable(args_arr[++i]);
                }
                else if (args_arr[i] == "-rightTable")
                {
                    m_inputRowsetRight.get()->Schema->SetTable(args_arr[++i]);
                }
                else
                {
                    unusedArgs->Add(args_arr[i]);
                }
            }

            m_combiner->Initialize(m_inputRowsetLeft.get(), m_inputRowsetRight.get(), unusedArgs->ToArray());
            m_enumerator = m_combiner->Rows->GetEnumerator();
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

            if (enumerator->MoveNext())
            {
                if (enumerator->Current == nullptr  || !Object::ReferenceEquals(m_combiner->DefaultRow->GetType(), enumerator->Current->GetType()))
                {
                    throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                }

                m_allocator.Reset();
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                SendMemoryLoadNotification(m_combiner);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputRowsetLeft)
            {
                m_inputRowsetLeft->WriteRuntimeStats(node);
            }
            if (m_inputRowsetRight)
            {
                m_inputRowsetRight->WriteRuntimeStats(node);
            }
        }

        virtual ~ScopeCombinerManagedImpl()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            System::IO::BinaryWriter^ checkpoint = gcnew System::IO::BinaryWriter(gcnew ScopeCosmosStream(&output.GetOutputer(), 0x400000, 2));
            m_combiner->DoStreamingCheckpoint(checkpoint);
            checkpoint->Write((int)SCOPECEP_CHECKPINT_MAGICNUMBER);
            checkpoint->Flush();
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            System::IO::BinaryReader^ checkpoint = gcnew System::IO::BinaryReader(gcnew ScopeCosmosStream(&input.GetInputer(), 0x400000, 2));
            m_combiner->LoadStreamingCheckpoint(checkpoint);
            CheckScopeCEPCheckpointMagicNumber(checkpoint);
        }
    };

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema>
    const char* const ScopeCombinerManagedImpl<InputSchemaLeft, InputSchemaRight, OutputSchema>::sm_className = "ScopeCombinerManagedImpl";

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, int UID>
    INLINE ScopeCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema> * ScopeCombinerManagedFactory::Make
    (
        OperatorDelegate<InputSchemaLeft> * leftChild,
        OperatorDelegate<InputSchemaRight> * rightChild
    )
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        return new ScopeCombinerManagedImpl<InputSchemaLeft, InputSchemaRight, OutputSchema>(managedUDO.get(),
                                                                                             managedUDO.args(),
                                                                                             leftChild,
                                                                                             rightChild,
                                                                                             &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal,
                                                                                             &InteropToManagedRowPolicy<InputSchemaLeft,UID>::Marshal,
                                                                                             &InteropToManagedRowPolicy<InputSchemaRight,UID>::Marshal);
    }

    template<typename InputSchema>
    class ScopeOutputerManagedImpl : public ScopeOutputerManaged<InputSchema>
    {
    protected:
        friend struct ScopeOutputerManagedFactory;

        InputSchema m_inputRow;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallType) (InputSchema &, ScopeRuntime::Row ^%, InteropAllocator *);

        ScopeTypedManagedHandle<ScopeInputRowset<InputSchema> ^> m_inputRowset;
        ScopeTypedManagedHandle<ScopeRuntime::Outputter ^>       m_outputer;
        ScopeTypedManagedHandle<Generic::List<String^> ^>        m_args;
        ScopeTypedManagedHandle<ScopeCosmosStream ^>             m_outputStream;
        ScopeTypedManagedHandle<System::IO::StreamWriter ^>      m_writer;

        ScopeOutputerManagedImpl(ScopeRuntime::Outputter ^ udo, Generic::List<String^>^ args, OperatorDelegate<InputSchema> * child, MarshalToManagedCallType marshalToManagedCall):
            m_outputer(udo),
            m_args(args)
        {
            m_inputRowset = gcnew ScopeInputRowset<InputSchema>(m_inputRow, child, udo, marshalToManagedCall);
        }

    public:
		virtual void CreateStream(std::string& outputName, SIZE_T bufSize, int bufCnt)
		{
			m_outputStream = gcnew ScopeCosmosStream(outputName, false, bufSize, bufCnt);			
		}

        virtual void Init()
        {
            m_outputStream->Init();

            m_writer = gcnew System::IO::StreamWriter(m_outputStream.get());

            // This method is deprecated but many UDOs are bound to it
            m_outputer->InitializeAtRuntime(m_inputRowset.get(), m_args->ToArray(), m_outputer->Schema);
        }

        virtual void Output()
        {
            m_outputer->Output(m_inputRowset.get(), m_writer.get(), m_args->ToArray());
        }

        virtual void Close()
        {
            if (m_writer)
            {
                m_writer->Close();
            }
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            RuntimeStats::WriteRowCount(root, (LONGLONG)m_inputRowset->RowCount);

            auto & node = root.AddElement("ScopeOutputerManagedImpl");

            if (m_inputRowset)
            {
                m_inputRowset->WriteRuntimeStats(node);
            }

            if (m_outputStream)
            {
                m_outputStream->WriteRuntimeStats(node);
            }
        }

        virtual ~ScopeOutputerManagedImpl()
        {
            // There only to ensure proper destruction when base class destructor is called
            if (m_outputStream)
            {
                try
                {
                    m_outputStream.reset();
                }
                catch (std::exception&)
                {
                    // ignore any I/O errors as we are destroying the object anyway
                }
            }
        }

        // Time in writing data
        virtual __int64 GetIOTime()
        {
            if (m_outputStream)
            {
                return m_outputStream->GetInclusiveTimeMillisecond();
            }

            return 0;
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw gcnew NotImplementedException("ScopeManagedOutputer doesn't support CEP checkpoint");
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw gcnew NotImplementedException("ScopeManagedOutputer doesn't support CEP checkpoint");
        }
    };

    // specialized rowset for streaming outputer. It splits the upstream rowset into multiple
    // batches, based on the checkpoint and the output channel flush requirement.
    // when split is needed, m_needPauseEnumeration will be set to true, so next round of GetNext will
    // return false and the execution will go back to ScopeOutputerManaged. The Outputer may flush the output stream
    // or do checkpoint based on what triggers the split, and need call Resume to start the next batch.
    template <typename InputSchema>
    ref class RowsetAdapterForStreamingManagedOutputer: public ScopeRowset
    {
        ScopeInputRowset<InputSchema>^ m_inputRowset;
        StreamingOutputChannel* m_pOutputChannel;
        int m_switchToNext;
        bool m_needCheckpoint;
        bool m_hasMoreRows;
        bool m_fromCheckpoint;

    public:
        RowsetAdapterForStreamingManagedOutputer(
            StreamingOutputChannel* pOutputChannel,
            ScopeInputRowset<InputSchema>^ inputRowset)
           : ScopeRowset(ManagedRow<InputSchema>().get())
        {
             m_inputRowset = inputRowset;
             m_pOutputChannel = pOutputChannel;
             m_hasMoreRows = true;
             if (!g_scopeCEPCheckpointManager->GetStartScopeCEPState().empty())
             {
                ScopeDateTime startTime = g_scopeCEPCheckpointManager->GetStartCTITime();
                (m_inputRowset->GetNativeRow()).ResetScopeCEPStatus(startTime, startTime.AddTicks(1), SCOPECEP_CTI_CHECKPOINT);
                m_inputRowset->MarshalToManagedRow();
                _outputRow = m_inputRowset->GetManagedRow();
                m_fromCheckpoint = true;
             }
        }

        virtual bool MoveNext() override
        {
            if (m_fromCheckpoint)
            {
                m_fromCheckpoint = false;
                return true;
            }

            if (m_switchToNext == 1)
            {
                m_switchToNext = 2;
                return false;
            }

            if (m_switchToNext == 2)
            {
                m_switchToNext = 0;
                m_needCheckpoint = false;
                return true;
            }

            if (m_needCheckpoint)
            {
                m_needCheckpoint = false;
                return true;
            }

            m_hasMoreRows = m_inputRowset->MoveNext();
            if (m_hasMoreRows)
            {
                _outputRow = m_inputRowset->GetManagedRow();
                if (m_inputRowset->GetNativeRow().IsScopeCEPCTI())
                {
                    if (!m_pOutputChannel->TryAdvanceCTI(m_inputRowset->GetNativeRow().GetScopeCEPEventStartTime(), ScopeDateTime::MaxValue, true))
                    {
                        m_switchToNext = 1;
                    }

                    if (m_inputRowset->GetNativeRow().GetScopeCEPEventType() == (UINT8)SCOPECEP_CTI_CHECKPOINT && 
                        g_scopeCEPCheckpointManager->IsWorthyToDoCheckpoint(m_inputRowset->GetNativeRow().GetScopeCEPEventStartTime()))
                    {
                        m_needCheckpoint = true;
                    }
                    g_scopeCEPCheckpointManager->UpdateLastCTITime(m_inputRowset->GetNativeRow().GetScopeCEPEventStartTime());
                }
            }

            return ((m_switchToNext > 0) || (m_hasMoreRows && !m_needCheckpoint));
        }

        void Resume()
        {
            ResetEnumerator();
        }

        bool NeedSwitchToNext() { return m_switchToNext > 0; }
        bool NeedCheckpoint() { return m_needCheckpoint; }
        bool HasMoreRows() { return m_hasMoreRows || m_fromCheckpoint; }

        void CompleteCheckpoint()
        {
        }

        void CompleteSwitch()
        {
            SCOPE_ASSERT(m_pOutputChannel->TryAdvanceCTI(m_inputRowset->GetNativeRow().GetScopeCEPEventStartTime(), ScopeDateTime::MaxValue, false));
        }
    };
    
    template<typename InputSchema>
    class StreamingOutputerManagedImpl: public ScopeOutputerManagedImpl<InputSchema>
    {
        StreamingOutputChannel* m_pOutputChannel;
        CRITICAL_SECTION  m_lock;
        int               m_runScopeCEPMode;
        OperatorDelegate<InputSchema> * m_child;
        std::string m_outputName;
    public:
        StreamingOutputerManagedImpl(ScopeRuntime::Outputter ^ udo, Generic::List<String^>^ args, OperatorDelegate<InputSchema> * child, MarshalToManagedCallType marshalToManagedCall, int runScopeCEPMode):
            ScopeOutputerManagedImpl(udo, args, child, marshalToManagedCall)
        {
            InitializeCriticalSection(&m_lock);
            m_runScopeCEPMode = runScopeCEPMode;
            m_child = child;
        }

        virtual ~StreamingOutputerManagedImpl()
        {
            DeleteCriticalSection(&m_lock);
        }

        static VOID CALLBACK AuToFlushRoutineInternal(PVOID args, BOOL TimerOrWaitFired)
        {
            StreamingOutputerManagedImpl * output = (StreamingOutputerManagedImpl *)args;
            output->Flush();
        }

        void Flush()
        {
            AutoCriticalSection aCs(&m_lock);
            m_outputStream->FlushAllData();
        }

        virtual void CreateStream(std::string& outputName, SIZE_T bufSize, int bufCnt)
        {
            m_outputName = outputName;
            m_outputStream = gcnew ScopeCosmosStreamWithLock(outputName, false, bufSize, bufCnt, &m_lock);
        }

        virtual void Init() override
        {
            ScopeOutputerManagedImpl<InputSchema>::Init();
            m_pOutputChannel = IOManager::GetGlobal()->GetStreamingOutputChannel(m_outputName);
            m_pOutputChannel->SetAllowDuplicateRecord(true);
        }

        virtual void Output() override
        {
            std::shared_ptr<void> autoFlushTimer;
            HANDLE handle = INVALID_HANDLE_VALUE;

            if (m_runScopeCEPMode == SCOPECEP_MODE_REAL &&
                CreateTimerQueueTimer(&handle, NULL, (WAITORTIMERCALLBACK)AuToFlushRoutineInternal, this, 500, 500, WT_EXECUTEINTIMERTHREAD))
            {
                autoFlushTimer.reset(handle, DeleteAutoFlushTimer);
            }

            ScopeTypedManagedHandle<RowsetAdapterForStreamingManagedOutputer<InputSchema>^> rowsetAdpater;
            rowsetAdpater = gcnew RowsetAdapterForStreamingManagedOutputer<InputSchema>(m_pOutputChannel, m_inputRowset);

            while (rowsetAdpater->HasMoreRows())
            {
                m_outputer->Output(rowsetAdpater.get(), m_writer.get(), m_args->ToArray());

                if (rowsetAdpater->NeedCheckpoint())
                {
                    std::string uri = g_scopeCEPCheckpointManager->GenerateCheckpointUri();
                    IOManager::GetGlobal()->AddOutputStream(uri, uri, "", ScopeTimeSpan::TicksPerWeek);
                    BinaryOutputStream checkpoint(uri, 0x400000, 2);
                    checkpoint.Init();
                    DoScopeCEPCheckpoint(checkpoint);
                    checkpoint.Finish();
                    checkpoint.Close();
                    g_scopeCEPCheckpointManager->UpdateCurrentScopeCEPState(&uri);
                    rowsetAdpater->CompleteCheckpoint();
                }

                if (rowsetAdpater->NeedSwitchToNext())
                {
                    EnterCriticalSection(&m_lock);
                    // flush the encoder in case it has data left
                    m_writer->Flush(); 
                    // flush the real stream --the Flush method on ScopeCosmosStream is changed 
                    // not to flush the data but only mark the line boundaries
                    // so we need call FlushAllData to ensure all the data really flush into the device
                    m_outputStream->FlushAllData(); 
                    rowsetAdpater->CompleteSwitch();
                    LeaveCriticalSection(&m_lock);
                }

                if (rowsetAdpater->HasMoreRows())
                {
                    rowsetAdpater->Resume();
                }
            }
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            Flush();
            m_outputStream->SaveState(output);
            System::IO::BinaryWriter^ checkpoint = gcnew System::IO::BinaryWriter(gcnew ScopeCosmosStream(&output.GetOutputer(), 0x400000, 2));
            m_outputer->DoStreamingCheckpoint(checkpoint);
            checkpoint->Write((int)SCOPECEP_CHECKPINT_MAGICNUMBER);
            checkpoint->Flush();
            m_child->DoScopeCEPCheckpoint(output);
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            m_outputStream->LoadState(input);
            System::IO::BinaryReader^ checkpoint = gcnew System::IO::BinaryReader(gcnew ScopeCosmosStream(&input.GetInputer(), 0x400000, 2));
            m_outputer->LoadStreamingCheckpoint(checkpoint);
            CheckScopeCEPCheckpointMagicNumber(checkpoint);
            m_child->LoadScopeCEPCheckpoint(input);
        }
    };

    template<typename InputSchema, int UID, int RunScopeCEPMode>
    ScopeOutputerManaged<InputSchema> * ScopeOutputerManagedFactory::Make(OperatorDelegate<InputSchema> * child)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        if (RunScopeCEPMode == SCOPECEP_MODE_NONE)
        {
            return new ScopeOutputerManagedImpl<InputSchema>(managedUDO.get(), 
                                                             managedUDO.args(), 
                                                             child, 
                                                             &InteropToManagedRowPolicy<InputSchema,UID>::Marshal);
        }
        else
        {
            return new StreamingOutputerManagedImpl<InputSchema>(managedUDO.get(), 
                managedUDO.args(), 
                child, 
                &InteropToManagedRowPolicy<InputSchema,UID>::Marshal,
                RunScopeCEPMode);
        }
    }
    
#pragma endregion ManagedOperators

    // Wrapper for ScopeMap Native
    template<typename NK, typename NV, typename MK, typename MV>
    public ref class ScopeMapWrapper : public Microsoft::SCOPE::Types::ScopeMap<MK, MV>
    {
    
    private:
        
        ScopeMapNative<NK, NV> *m_inner; // ScopeMapWrapper does not own native scopemap  
        RowEntityAllocator* m_allocator;

        bool IsNull()
        {
            return m_inner == nullptr || m_inner->IsNull();
        }

        ref class InnerEnumerator: public Generic::IEnumerator<Generic::KeyValuePair<MK, MV>>

        {
        
        private:

            ScopeMapWrapper<NK, NV, MK, MV>^ m_map;
            typename ScopeMapNative<NK, NV>::const_iterator* m_iter;
            bool m_fStart;
            bool m_eof;

        public:

            InnerEnumerator(ScopeMapWrapper<NK, NV, MK, MV>^ map)
            {
                m_map = map;
                m_iter = m_map->IsNull() ? nullptr : new ScopeMapNative<NK, NV>::const_iterator(m_map->m_inner->begin());
                m_fStart = false;
                m_eof = false;
            }

            ~InnerEnumerator()
            {
                this->!InnerEnumerator();
            }

            !InnerEnumerator()
            {              
                if (m_iter != nullptr) delete m_iter;
                m_iter = nullptr;
            }

            property Generic::KeyValuePair<MK, MV> Current
            {
                virtual Generic::KeyValuePair<MK, MV> get() = Generic::IEnumerator<Generic::KeyValuePair<MK, MV>>::Current::get
                {
                    MK manageKey;
                    MV manageValue;
                    const NK& nativeKey = m_iter->Key();
                    const NV& nativeValue = m_iter->Value();
                 
                    ScopeManagedInterop::CopyToManagedColumn(nativeKey, manageKey);
                    ScopeManagedInterop::CopyToManagedColumn(nativeValue, manageValue);

                    return Generic::KeyValuePair<MK, MV>(manageKey, manageValue);
                }
            }

            property Object^ Current2
            {
                virtual Object^ get() = IEnumerator::Current::get
                { 
                    return Current; 
                }
            }

            virtual bool MoveNext() = Generic::IEnumerator<Generic::KeyValuePair<MK, MV>>::MoveNext
            {
                if (m_map->IsNull())
                {
                    return false;
                }

                // never pass the end cursor
                if (m_eof)
                {
                    return false;
                }
                
                if (m_fStart)
                {
                    m_eof = (++(*m_iter) == m_map->m_inner->end());
                }
                else
                {
                    m_fStart = true;
                    m_eof = (*m_iter == m_map->m_inner->end());
                }
                
                return !m_eof;
            }

            virtual void Reset() = Generic::IEnumerator<Generic::KeyValuePair<MK, MV>>::Reset
            {                
                throw gcnew NotImplementedException();
            }

        };     

    public:
    
        ScopeMapWrapper()
        {
            m_inner = nullptr;
            m_allocator = new RowEntityAllocator(Configuration::GetGlobal().GetMaxVariableColumnSize(), "ScopeMapWrapper", RowEntityAllocator::ColumnContent);
        }

        ~ScopeMapWrapper()
        {
            this->!ScopeMapWrapper();
        }

        !ScopeMapWrapper()
        {
            m_inner = nullptr;
            delete m_allocator;
            m_allocator = nullptr;
        }

        void SetScopeMapNative(ScopeMapNative<NK, NV>* m)
        {
            m_inner = m;
        }

        ScopeMapNative<NK, NV>* GetScopeMapNative()
        {
            return m_inner;
        }
        
        // inherit ScopeMap interface 
        virtual property int Count
        {
            int get() override
            {
                return IsNull() ? 0 : m_inner->count();
            }
        }

        // inherit ScopeMap interface 
        virtual bool ContainsKey(MK mkey) override
        {
            if (IsNull())
            {
                return false;
            }
            
            NK nativeKey;
            ScopeManagedInterop::CopyToNativeColumn(nativeKey, mkey, m_allocator);
            bool f = m_inner->ContainsKey(nativeKey);
            m_allocator->Reset();
            return f;
        }

        // inherit ScopeMap interface 
        virtual property MV default[MK]
        {
            MV get(MK mkey) override
            {
                MV manageValue;
                NK nativeKey;
                ScopeManagedInterop::CopyToNativeColumn(nativeKey, mkey, m_allocator);
                const NV* nativeValue = IsNull() ? nullptr : m_inner->TryGetValue(nativeKey);
                if (nativeValue != nullptr)
                {
                    ScopeManagedInterop::CopyToManagedColumn(*nativeValue, manageValue);
                }

                m_allocator->Reset();
                return manageValue;
            }
        }

        // inherit ScopeMap interface 
        virtual Generic::IEnumerator<Generic::KeyValuePair<MK, MV>>^ GetEnumerator() override
        {
            return gcnew InnerEnumerator(this);
        }

        // inherit ScopeMap interface 
        virtual IEnumerator^ EnumerableGetEnumerator() = IEnumerable::GetEnumerator
        {
            return GetEnumerator();
        }

        // inherit ScopeMap interface
        virtual property Microsoft::SCOPE::Types::ScopeArray<MK>^ Keys
        {
            Microsoft::SCOPE::Types::ScopeArray<MK>^ get() override
            {
                array<MK>^ keys = gcnew array<MK>(this->Count);
                int idx = 0;
                for each(Generic::KeyValuePair<MK, MV>^ kv in this)
                {
                    keys[idx++] = kv->Key;
                }

                return gcnew Microsoft::SCOPE::Types::ScopeArray<MK>(keys);
            }
        }

        // inherit ScopeMap interface
        virtual property Microsoft::SCOPE::Types::ScopeArray<MV>^ Values       
        {
            Microsoft::SCOPE::Types::ScopeArray<MV>^ get() override
            {
                array<MV>^ values = gcnew array<MV>(this->Count);
                int idx = 0;
                for each(Generic::KeyValuePair<MK, MV>^ kv in this)
                {
                    values[idx++] = kv->Value;
                }

                return gcnew Microsoft::SCOPE::Types::ScopeArray<MV>(values);
            }
        }
        
    }; // ScopeMapWrapper

    // ScopeArrayWrapper
    template<typename NT, typename MT>
    public ref class ScopeArrayWrapper : public Microsoft::SCOPE::Types::ScopeArray<MT>
    {
    
    private:
        
        ScopeArrayNative<NT> *m_inner; // ScopeArrayWrapper reference native one
        RowEntityAllocator *m_allocator;

        bool IsNull()
        {
            return m_inner == nullptr || m_inner->IsNull();
        }
    
        ref class InnerEnumerator: public Generic::IEnumerator<MT>
        {
        
        private:

            ScopeArrayWrapper<NT, MT>^ m_array;
            typename ScopeArrayNative<NT>::const_iterator *m_iter;
            bool m_fStart;
            bool m_eof;

        public:

            InnerEnumerator(ScopeArrayWrapper<NT, MT>^ array)
            {
                m_array = array;
                m_iter = m_array->IsNull() ? nullptr : new ScopeArrayNative<NT>::const_iterator(m_array->m_inner->begin());
                m_fStart = false;
                m_eof = false;
            }

            ~InnerEnumerator()
            {
                this->!InnerEnumerator();
            }

            !InnerEnumerator()
            {
                if (m_iter != nullptr) delete m_iter;
                m_iter = nullptr;
            }

            property MT Current
            {
                virtual MT get() = Generic::IEnumerator<MT>::Current::get
                {
                    MT manageValue;
                    ScopeManagedInterop::CopyToManagedColumn(m_iter->Value(), manageValue);
                    return manageValue;
                }
            }

            property Object^ Current2
            {
                virtual Object^ get() = IEnumerator::Current::get
                { 
                    return Current; 
                }
            }

            virtual bool MoveNext() = Generic::IEnumerator<MT>::MoveNext
            {
                if (m_array->IsNull())
                {
                    return false;
                }               

                // never pass the end cursor
                if (m_eof)
                {
                    return false;
                }
                
                if (m_fStart)
                {
                    m_eof = (++(*m_iter) == m_array->m_inner->end());
                }
                else
                {
                    m_fStart = true;
                    m_eof = (*m_iter == m_array->m_inner->end());
                }

                return !m_eof;
            }

            virtual void Reset() = Generic::IEnumerator<MT>::Reset
            {                
                throw gcnew NotImplementedException();
            }
            
        }; //InnerEnumerator

    public:
    
        ScopeArrayWrapper()
        {
            m_inner = nullptr;
            m_allocator = new RowEntityAllocator(Configuration::GetGlobal().GetMaxVariableColumnSize(), "ScopeArrayWrapper", RowEntityAllocator::ColumnContent);
        }

        ~ScopeArrayWrapper()
        {
            this->!ScopeArrayWrapper();
        }

        !ScopeArrayWrapper()
        {
            m_inner = nullptr;
            delete m_allocator;
            m_allocator = nullptr;
        }

        void SetScopeArrayNative(ScopeArrayNative<NT>* array)
        {
            m_inner = array;
        }

        ScopeArrayNative<NT>* GetScopeArrayNative()
        {
            return m_inner;
        }
        
        // inherit ScopeArray interface 
        virtual property int Count
        {
            int get()  override
            {
                return IsNull() ? 0 : m_inner->count();
            }
        }

        // inherit ScopeArray interface 
        virtual bool Contains(MT mvalue) override
        {
            if (IsNull())
            {
                return false;
            }
            
            NT nativeValue;
            ScopeManagedInterop::CopyToNativeColumn(nativeValue, mvalue, m_allocator);
            bool result = m_inner->Contains(nativeValue);            
            m_allocator->Reset();
            return result;
        }

        // inherit ScopeArray interface 
        virtual property MT default[int]
        {
            MT get(int index) override
            {
                if (index < 0 || index >= Count)
                {
                    throw gcnew ArgumentOutOfRangeException("index");   
                }

                MT manageValue;
                ScopeManagedInterop::CopyToManagedColumn((*m_inner)[index], manageValue);
                return manageValue;
            }
        }

        // inherit ScopeArray interface 
        virtual Generic::IEnumerator<MT>^ GetEnumerator() override
        {
            return gcnew InnerEnumerator(this);
        }

        // inherit ScopeArray interface 
        virtual IEnumerator^ EnumerableGetEnumerator() = IEnumerable::GetEnumerator
        {
            return GetEnumerator();
        }           
        
    };// ScopeArrayWrapper

#pragma region SQLIP

    // Helper class for getting one column. Given a starting address in native space, helper method will copy it to managed space then return managed value. 
    public ref class ColumnGetter
    {
    
    public:

        generic<typename T>
        static T GetColumn(ScopeEngine::ColumnType t, BYTE* start, UINT offset)
        {
            BYTE *r = start + offset;
            switch(t)
            {
                // String
                case ScopeEngine::ColumnType::T_String:
                    return (T)Getter<FString, System::String^>(r);

                // Binary
                case ScopeEngine::ColumnType::T_Binary:
                    return (T)Getter<FBinary, cli::array<System::Byte>^>(r);

                // Boolean
                case ScopeEngine::ColumnType::T_Boolean:
                    return (T)Getter<bool, System::Boolean>(r);

                // BooleanQ
                case ScopeEngine::ColumnType::T_BooleanQ:
                    return (T)Getter<NativeNullable<bool>, System::Nullable<System::Boolean>>(r);

                // Integer
                case ScopeEngine::ColumnType::T_Integer:
                    return (T)Getter<int, System::Int32>(r);

                // IntegerQ
                case ScopeEngine::ColumnType::T_IntegerQ:
                    return (T)Getter<NativeNullable<int>, System::Nullable<System::Int32>>(r);

                // Long
                case ScopeEngine::ColumnType::T_Long:
                    return (T)Getter<__int64, System::Int64>(r);

                // LongQ
                case ScopeEngine::ColumnType::T_LongQ:
                    return (T)Getter<NativeNullable<__int64>, System::Nullable<System::Int64>>(r);

                // Float
                case ScopeEngine::ColumnType::T_Float:
                    return (T)Getter<float, System::Single>(r);

                // FloatQ
                case ScopeEngine::ColumnType::T_FloatQ:                
                    return (T)Getter<NativeNullable<float>, System::Nullable<System::Single>>(r);

                // Double
                case ScopeEngine::ColumnType::T_Double:
                    return (T)Getter<double, System::Double>(r);

                // DoubleQ
                case ScopeEngine::ColumnType::T_DoubleQ:
                    return (T)Getter<NativeNullable<double>, System::Nullable<System::Double>>(r);

                // DateTime
                case ScopeEngine::ColumnType::T_DateTime:
                    return (T)Getter<ScopeDateTime, System::DateTime>(r);

                // DateTimeQ
                case ScopeEngine::ColumnType::T_DateTimeQ:
                    return (T)Getter<NativeNullable<ScopeDateTime>, System::Nullable<System::DateTime>>(r);

                // Decimal
                case ScopeEngine::ColumnType::T_Decimal:
                    return (T)Getter<ScopeDecimal, System::Decimal>(r);

                // DecimalQ
                case ScopeEngine::ColumnType::T_DecimalQ:
                    return (T)Getter<NativeNullable<ScopeDecimal>, System::Nullable<System::Decimal>>(r);

                // Byte
                case ScopeEngine::ColumnType::T_Byte:
                    return (T)Getter<unsigned char, System::Byte>(r);

                // ByteQ
                case ScopeEngine::ColumnType::T_ByteQ:
                    return (T)Getter<NativeNullable<unsigned char>, System::Nullable<System::Byte>>(r);

                // SByte
                case ScopeEngine::ColumnType::T_SByte:
                    return (T)Getter<char, System::SByte>(r);

                // SByteQ
                case ScopeEngine::ColumnType::T_SByteQ:
                    return (T)Getter<NativeNullable<char>, System::Nullable<System::SByte>>(r);

                // Short
                case ScopeEngine::ColumnType::T_Short:
                    return (T)Getter<short, System::Int16>(r);

                // ShortQ
                case ScopeEngine::ColumnType::T_ShortQ:
                    return (T)Getter<NativeNullable<short>, System::Nullable<System::Int16>>(r);

                // UShort
                case ScopeEngine::ColumnType::T_UShort:
                    return (T)Getter<unsigned short, System::UInt16>(r);

                // UShortQ
                case ScopeEngine::ColumnType::T_UShortQ:
                    return (T)Getter<NativeNullable<unsigned short>, System::Nullable<System::UInt16>>(r);

                // UInt
                case ScopeEngine::ColumnType::T_UInt:
                    return (T)Getter<unsigned int, System::UInt32>(r);

                // UIntQ
                case ScopeEngine::ColumnType::T_UIntQ:
                    return (T)Getter<NativeNullable<unsigned int>, System::Nullable<System::UInt32>>(r);

                // ULong
                case ScopeEngine::ColumnType::T_ULong:
                    return (T)Getter<unsigned __int64, System::UInt64>(r);

                // ULongQ
                case ScopeEngine::ColumnType::T_ULongQ:
                    return (T)Getter<NativeNullable<unsigned __int64>, System::Nullable<System::UInt64>>(r);

                // Guid
                case ScopeEngine::ColumnType::T_Guid:
                    return (T)Getter<ScopeGuid, System::Guid>(r);

                // GuidQ
                case ScopeEngine::ColumnType::T_GuidQ:
                    return (T)Getter<NativeNullable<ScopeGuid>, System::Nullable<System::Guid>>(r);
                    
                default:
                
                    throw gcnew ArgumentException("bad type when getting column: ", T::typeid->ToString());
            }           
        }

        template<typename NT, typename MT>
        static MT Getter(BYTE *address)
        {
            NT n = *((NT*)address);
            MT m;
            ScopeManagedInterop::CopyToManagedColumn(n, m);
            return m;
        }
        
    };

    // Describe column's offset and type inside a native row
    public value struct ColumnOffsetId
    {
        // offset from row start point
        UINT offset;

        // column type
        ScopeEngine::ColumnType typeId;

        ColumnOffsetId(UINT o, ScopeEngine::ColumnType t)
        {
            offset = o;
            typeId = t;
        }

        bool IsComplexType()
        {
            return typeId == ScopeEngine::ColumnType::T_Map || 
                   typeId == ScopeEngine::ColumnType::T_Array || 
                   typeId == ScopeEngine::ColumnType::T_Struct;
        }
    }; // end of columnOffsetId

    public value struct SqlIpValue
    {
        // real value
        System::Object^ m_value;

        // valid or not
        bool m_valid;
        
    }; // end of SqlIpValue

    // Column definition
    public ref class SqlIpColumn : public IColumn
    {

    public:
    
        // Inherit  from IColumn.Name, column name
        virtual property System::String^ Name 
        { 
            System::String^ get() override
            {
                return m_name;
            }
        }

        // Inherit  from IColumn.Type, column type
        virtual property System::Type^ Type 
        { 
            System::Type^ get() override
            {
                return m_type;
            }
        }

        // default value
        virtual property System::Object^ DefaultValue
        {
            System::Object^ get() override
            {
                return m_defaultValue;
            }
        }

        // Constructor
        SqlIpColumn(System::String^ name, System::Type^ type, bool readonly)
            : m_name(name), 
              m_type(type),
              m_readonly(readonly)
        {
            m_defaultValue = type->IsValueType ? System::Activator::CreateInstance(type) : nullptr;            
        }

        // Readonly
        property bool ReadOnly
        {
            bool get()
            {
                return m_readonly;
            }
        }
        
    private:
    
        // column name
        System::String^ m_name;

        // clr type
        System::Type^   m_type;

        // default value
        System::Object^ m_defaultValue;

        // readonly flag
        bool m_readonly;
        
    }; // end of SqlIpColumn

    // static mehtod to create IUpdatableRow, break the circular reference between SqlIpRow and SqlIpUpdatableRow
    static IUpdatableRow^ CreateSqlIpUpdatableRow(IRow^ row);

    // static method to create IRow, break the circular reference between SqlIpRow and SqlIpSchema
    static IRow^ CreateSqlIpRow(ISchema^ schema, cli::array<SqlIpValue>^ values);

    // typedef a function for column getter
    typedef System::Object^ (*SqlIpComplexColumnGetter) (int, BYTE*);
    
    /// <summary/>
    public ref class SqlIpSchema : public ISchema
    {

    public:

        // Inherit from ISchema.IndexOf(),  return column index given column name 
        virtual int IndexOf(System::String^ columnName) override
        {
            int index = -1;
            if (this->m_offset->TryGetValue(columnName, index))
            {
                return index;
            }
            else
            {
                throw gcnew ArgumentException("Column name not exist in schema: ", columnName);
            }
        }

        // Inherit from ISchema.Defaults, return a IRow with each column having default value
        // Note: This can be cached per instance since schema is immutable
        virtual property IRow^ Defaults 
        { 
            IRow^ get() override
            {
                // Get default value for all columns and cache
                if (m_default == nullptr)
                {
                    auto v = gcnew cli::array<SqlIpValue>(this->m_columnCount);
                    for(int idx = 0; idx < this->m_columnCount; ++idx)
                    {
                        v[idx].m_value = m_columns[idx]->DefaultValue;
                        v[idx].m_valid = true;
                    }

                    m_default = CreateSqlIpRow(this, v);
                }

                return m_default;
            }
        }

        // Inherit from ISchema.Count, return column count
        virtual property int Count
        {
            int get() override
            {
                return m_columnCount;
            }
        }

        // Inherit from ISchema[idx], return column object given index
        virtual property IColumn^ default[int] 
        {
            IColumn^ get(int index) override
            {
                if (index > m_columnCount || index < 0)
                {
                    throw gcnew ArgumentOutOfRangeException("index is out of range: ", index.ToString());
                }

                return m_columns[index];
            }
        }

        // inherit ISchema enumerator
        virtual Generic::IEnumerator<IColumn^>^ GetEnumerator() override
        {
            return gcnew InnerEnumerator(this);
        }

        // inherit ISchema enumerator
        virtual IEnumerator^ EnumerableGetEnumerator() = IEnumerable::GetEnumerator
        {
            return GetEnumerator();
        }
        
        // Constructor.
        SqlIpSchema(Generic::IEnumerable<SqlIpColumn^>^ columns, SqlIpComplexColumnGetter complexColumnGetter)
        {            
            m_offset = gcnew Generic::Dictionary<System::String^, int>();
            m_columns = gcnew Generic::List<SqlIpColumn^>();

            int idx = 0;
            for each(SqlIpColumn^ c in columns)
            {
                m_offset->Add(c->Name, idx++);
                m_columns->Add(c);
            }

            m_columnCount = m_columns->Count;
            m_default = nullptr;
            m_complexColumnGetter = complexColumnGetter;
        }

        System::Object^ GetComplexColumn(int index, BYTE* address)
        {
            return (*m_complexColumnGetter)(index, address);
        }

    private:

        // enumerator    
        ref class InnerEnumerator: public Generic::IEnumerator<IColumn^>
        {
        
        private:

            SqlIpSchema^ m_schema;
            int          m_columnCount;
            int          m_curIdx;
            
        public:

            InnerEnumerator(SqlIpSchema^ schema)
            {
                this->m_schema = schema;
                this->m_curIdx = -1;
                this->m_columnCount = (schema == nullptr) ? 0 : schema->Count;
            }

            ~InnerEnumerator()
            {
                this->!InnerEnumerator();
            }

            !InnerEnumerator()
            {
                this->m_schema = nullptr;
            }

            property IColumn^ Current
            {
                virtual IColumn^ get() = Generic::IEnumerator<IColumn^>::Current::get
                {
                    return m_schema[m_curIdx];
                }
            }

            property Object^ Current2
            {
                virtual Object^ get() = IEnumerator::Current::get
                { 
                    return Current; 
                }
            }

            virtual bool MoveNext() = Generic::IEnumerator<IColumn^>::MoveNext
            {
                if (m_curIdx+1 == m_columnCount)
                {
                    return false;
                }
                else
                {
                    m_curIdx++;
                    return true;
                }                                
            }

            virtual void Reset() = Generic::IEnumerator<IColumn^>::Reset
            {                
                throw gcnew NotImplementedException();
            }
            
        }; // end of InnerEnumerator

        // name -> idx mapping
        Generic::Dictionary<System::String^, int>^ m_offset;

        // columns list
        Generic::List<SqlIpColumn^>^ m_columns;

        // column count
        int m_columnCount;

        // array of default value for all columns
        IRow^ m_default;

        // a function pointer to manage row's column getter
        SqlIpComplexColumnGetter m_complexColumnGetter;
        
    }; // end of SqlIpSchema         

    /// <summary>
    /// readonly row
    /// </summary>    
    public ref class SqlIpRow : public IRow
    {

    public:

        // inherit IRow.Schema
        virtual property ISchema^ Schema
        {
            ISchema^ get() override
            { 
                return this->m_schema; 
            }
        }        

        // Inherit IRow.Get<T>      
        generic<typename T>
        virtual T Get(int index) override
        {
            this->Validate<T>(index);
            System::Object^ v; 
            
            if (QueryCache(index, v))
            {
                return (T)v;
            }
            else
            {
                ColumnOffsetId oid = m_columnOffset[index];                
                T result;
                
                if (oid.IsComplexType())
                {
                    result = (T)(m_schema->GetComplexColumn(index, this->m_rowPtr + oid.offset));
                }
                else
                {
                    result = ColumnGetter::GetColumn<T>(oid.typeId, this->m_rowPtr, oid.offset);
                }

                if(!T::typeid->IsValueType)
                {
                    InsertCache(index, (System::Object^)result);
                }
                
                return result;
            }
        }

        // Inherit IRow::Get<T>
        generic<typename T>
        virtual T Get(System::String^ columnName) override
        {
            // Delegate
            return this->Get<T>(this->m_schema->IndexOf(columnName));
        }

        // Inherit IRow.AsUpdatable()
        virtual IUpdatableRow^ AsUpdatable() override
        {
            return CreateSqlIpUpdatableRow(this);
        }
        
        // constructor
        SqlIpRow(ISchema^ schema, cli::array<SqlIpValue>^ values)
        {
            SCOPE_ASSERT(schema->Count == values->Length);
            
            this->m_schema = (SqlIpSchema^)schema;
            this->m_columnCount = schema->Count;
            this->m_cachedValues = values;
            this->m_rowPtr = nullptr;
            this->m_columnOffset = nullptr;
            this->m_cookie = s_default_cookie;
        }

        // constructor
        SqlIpRow(ISchema^ schema, cli::array<ColumnOffsetId>^ offsetId, BYTE* rowPtr)
        {
            SCOPE_ASSERT(schema->Count == offsetId->Length);
        
            this->m_schema = (SqlIpSchema^)schema;
            this->m_columnCount = schema->Count;
            this->m_cachedValues = gcnew cli::array<SqlIpValue>(this->m_columnCount);
            this->m_rowPtr = rowPtr;
            this->m_columnOffset = offsetId;
            this->m_cookie = s_default_cookie;
        }

        System::Object^ Cookie()
        {
            return m_cookie;
        }

        void SetCookie(System::Object^ cookie)
        {
            m_cookie = cookie;
        }

        static System::Object^ s_default_cookie = gcnew System::String("default cookie");
        
    private:

        SqlIpRow()
        {
        }
        
        generic<typename T>
        void Validate(int index)
        {
            if (index >= this->m_columnCount || index < 0)
            {
                System::String^ err = System::String::Format("Index out of range in Get<T> method, 0 based index: {0}, boundary: {1}", 
                                                             index, 
                                                             this->m_columnCount);
                                                             
                throw gcnew ArgumentOutOfRangeException(err);               
            }
            
            System::Type^ t = m_schema[index]->Type;
            if (T::typeid != t && !T::typeid->IsAssignableFrom(t))  /// TODO: IsAssignableFrom performance over lookup table
            {
                System::String^ err = System::String::Format("Bad type in Get<T> method, column: {0}, index: {1}, expected: {2}, actual: {3}", 
                                                             m_schema[index]->Name,
                                                             index, 
                                                             t->ToString(), 
                                                             T::typeid->ToString());
                throw gcnew ArgumentException(err);
            }
        }

        void InsertCache(int idx, System::Object^ v)
        {
            if (m_cachedValues != nullptr)
            {
                m_cachedValues[idx].m_valid = true;
                m_cachedValues[idx].m_value = v;
            }
        }

        bool QueryCache(int idx, System::Object^ %v)
        {
            if (m_cachedValues == nullptr)
            {
                return false;
            }
            
            if (m_cachedValues[idx].m_valid)
            {
                v = m_cachedValues[idx].m_value;
                return true;
            }
            else
            {
                return false;
            }
        }

        // schema
        SqlIpSchema^ m_schema; 

        // all column values
        cli::array<SqlIpValue>^ m_cachedValues;

        // column count
        int m_columnCount;

        // native row pointer
        BYTE* m_rowPtr;

        // column offet
        cli::array<ColumnOffsetId>^ m_columnOffset;

        // parent who own this instance
        System::Object^ m_cookie;
        
    }; // end of SqlIpRow

    /// <summary>
    /// updatable row
    /// </summary>
    public ref class SqlIpUpdatableRow : public IUpdatableRow
    {

    public:
    
        // Inherit IUpdatableRow.Set<T>(string, T)
        generic<typename T>
        virtual void Set(System::String^ column, T value) override
        {
            //Delegate
            this->Set<T>(this->Schema->IndexOf(column), value);
        }
        
        // Inherit IUpdatableRow.Set<T>(int, T)
        generic<typename T>
        virtual void Set(int index, T value) override
        {
            this->Validate<T>(index, value);
            this->SetInternal<T>(index, value);
        }

        // Inherit IUpdatableRow.Schema
        virtual property ISchema^ Schema 
        { 
            ISchema^ get() override
            { 
                return this->m_row->Schema; 
            }
        }

        // Inherit IUpdatableRow.Get<T>(int)
        generic<typename T>
        virtual T Get(int index) override
        {
            // Delegate
            return this->m_row->Get<T>(index);   
        }

        // Inherit IUpdatableRow::Get<T>(string)
        generic<typename T>
        virtual T Get(System::String^ column) override
        {
            // Delegate
            return this->m_row->Get<T>(column);
        }

        // Inherit IUpdatableRow.AsReadOnly()
        virtual IRow^ AsReadOnly() override
        {
            //Byref, so future changes don't modify the same row handed out
            this->m_byref = true;

            //Assign cookie for each IRow hand out
            this->m_row->SetCookie(this->m_cookie);
            return this->m_row;
        }

        // constructor
        SqlIpUpdatableRow(IRow^ row)
        { 
            SCOPE_ASSERT(row != nullptr);
            SCOPE_ASSERT(row->GetType() == SqlIpRow::typeid);
            
            this->m_row     = (SqlIpRow^)row;
            this->m_byref   = true;
            this->m_changed = nullptr;
            this->m_columnCount = row->Schema->Count; 
            this->m_cookie = (this->m_row->Cookie() == SqlIpRow::s_default_cookie) ? (System::Object^)this : this->m_row->Cookie();
        }

        // Reset
        void Reset()
        {
            this->m_row     = (SqlIpRow^)this->m_row->Schema->Defaults;          
            this->m_byref   = true;
            this->m_changed = nullptr;
            this->m_columnCount = this->m_row->Schema->Count;    
            this->m_cookie = (System::Object^)this;
        }

        // copy columns from src, destinated column ordinal. caller should guard parameter valid. 
        void CopyColumns(IRow^ src, cli::array<int>^ srcOrdinal, cli::array<int>^ dstOrdinal)
        {               
            for(int idx = 0; idx < srcOrdinal->Length; ++idx)
            {
                auto value = src->Get<System::Object^>(srcOrdinal[idx]);
                this->SetInternal(dstOrdinal[idx], value);
            }
        }
        
    private:

        SqlIpUpdatableRow()
        {
        }
        
        generic<typename T>
        void SetInternal(int index, T value)
        {
            // Byref
            if(this->m_byref)
            {
                // deferred until actualy needed (first set)
                this->m_changed = this->CopyValues();
                this->m_row     = gcnew SqlIpRow(this->Schema, this->m_changed);
                this->m_byref   = false;
            }

            this->m_changed[index].m_value = value; 
        }
        
        generic<typename T>
        void Validate(int index, T value)
        {
            // validate argument
            if (index >= this->m_columnCount || index < 0)
            {
                System::String^ err = System::String::Format("Index out of range in Set<T> method, 0 based index: {0}, boundary: {1}", 
                                                             index, 
                                                             this->m_columnCount);
                                                             
                throw gcnew ArgumentOutOfRangeException(err);

            }

            // validate readonly property
            SqlIpSchema^ schema = (SqlIpSchema^)(this->m_row->Schema);
            if (((SqlIpColumn^)schema[index])->ReadOnly)
            {
                System::String^ err = System::String::Format("Can not set readonly column, column: {0}, index: {1}", 
                                                             schema[index]->Name,
                                                             index);            
                throw gcnew InvalidOperationException(err);
            }

            // validate type
            System::Type^ t = schema[index]->Type;
            System::Type^ type = (value == nullptr? T::typeid : value->GetType());

            if ((value == nullptr && !IsNullable(t)) || (value != nullptr && type != t && !t->IsAssignableFrom(type))) /// TODO: IsAssignableFrom performance over lookup table
            {
                System::String^ err = System::String::Format("Bad type in Set<T> method, column: {0}, index: {1}, expected: {2}, actual: {3}", 
                                                             schema[index]->Name,
                                                             index, 
                                                             t->ToString(), 
                                                             type->ToString());
                throw gcnew ArgumentException(err);
            }
        }

        bool IsNullable(System::Type^ type)
        {
            if (!type->IsValueType)
            {
                 return true; // ref-type
            }

            if (System::Nullable::GetUnderlyingType(type) != nullptr)
            {
                 return true; // Nullable<T>
            }

            return false; // value-type
        }

        /// <summary/>
        cli::array<SqlIpValue>^ CopyValues()
        {       
            auto copy = gcnew cli::array<SqlIpValue>(this->m_columnCount);
            if(this->m_changed != nullptr)
            {
                System::Array::Copy(this->m_changed, copy, copy->Length);
            }
            else
            {
                for(int i = 0; i < copy->Length; i++)
                {
                    copy[i].m_value = this->m_row->Get<System::Object^>(i);
                    copy[i].m_valid = true;
                }
            }
             
            return copy;
        }

        // Internal IRow
        SqlIpRow^ m_row;

        // flag to indicate whether holding reference to internal row
        bool m_byref;

        // a cache holding all columns value
        cli::array<SqlIpValue>^ m_changed;    

        // column count
        int m_columnCount;

        // cookie
        System::Object^ m_cookie;

    }; // end of SqlIpUpdatableRow
                
    static INLINE IUpdatableRow^ CreateSqlIpUpdatableRow(IRow^ row)
    {
        return gcnew SqlIpUpdatableRow(row);
    }

    static INLINE IRow^ CreateSqlIpRow(ISchema^ schema, cli::array<SqlIpValue>^ values)
    {
        return gcnew SqlIpRow(schema, values);
    }
    
    static INLINE void CheckSqlIpUdoYieldRow(IRow^ row, System::Object^ explicitCookie)
    {
        if (row == nullptr || 
            row->GetType() != SqlIpRow::typeid ||
            (((SqlIpRow^)row)->Cookie() != SqlIpRow::s_default_cookie && ((SqlIpRow^)row)->Cookie() != explicitCookie)
            )
        {
            throw gcnew InvalidOperationException("Yielded IRow object is not the instance from output.AsReadOnly() or original IRow");
        }
    }          

    ref class SqlIpRowset abstract : public IRowset, public Generic::IEnumerable<IRow^>
    {
        ref class SqlIpRowsetEnumerator : public Generic::IEnumerator<IRow^>
        {
            bool                      m_hasRow;
            SqlIpRowset^              m_inputRowset;
            unsigned __int64          m_rowCount;

        public:
            SqlIpRowsetEnumerator(SqlIpRowset^ inputRowset) :
                m_inputRowset(inputRowset), 
                m_hasRow(false),
                m_rowCount(0)
            {
            }

            ~SqlIpRowsetEnumerator()
            {
            }

            property unsigned __int64 RowCount
            {
                unsigned __int64 get()
                {
                    return m_rowCount;
                }
            }

            property IRow^ Current
            {
                virtual IRow^ get()
                {
                    return m_hasRow ? m_inputRowset->m_currentRow : nullptr;
                }
            }

            property Object^ CurrentBase
            {
                virtual Object^ get() sealed = IEnumerator::Current::get
                {
                    return Current;
                }
            }
        
            virtual bool MoveNext()
            {
                if ((m_hasRow = m_inputRowset->MoveNext()) == true)
                {
                    m_rowCount++;
                }

                return m_hasRow;
            }
        
            virtual void Reset()
            {
                throw gcnew NotImplementedException();
            }
        };

        SqlIpRowsetEnumerator^ m_enumeratorOutstanding;        

    protected:

        SqlIpRow^ m_currentRow;
        SqlIpSchema^ m_schema;
        cli::array<ColumnOffsetId>^ m_columnOffset;
        
        //
        // SqlIpInputKeySet which is used for reducer needs distinct enuemrator for each key range
        //
        void ResetEnumerator()
        {
            m_enumeratorOutstanding = nullptr;
        }
        
    public:
        
        SqlIpRowset(SqlIpSchema^ schema, cli::array<ColumnOffsetId>^ offsetId) 
            : m_schema(schema),
              m_columnOffset(offsetId),
              m_enumeratorOutstanding(nullptr)
        {
        }

        virtual property Generic::IEnumerable<IRow^>^ Rows 
        {
            Generic::IEnumerable<IRow^>^ get() override
            {
                return this;
            }
        }

        virtual property ISchema^ Schema 
        {
            ISchema^ get() override
            {
                return m_schema;
            }
        }

        //
        // IEnumerable<T> part
        //
        virtual Generic::IEnumerator<IRow^>^ GetEnumerator() sealed = Generic::IEnumerable<IRow^>::GetEnumerator
        {
            if (m_enumeratorOutstanding != nullptr)
            {
                throw gcnew InvalidOperationException("User Error: Multiple instances of enumerators are not supported on input RowSet.Rows. The input Rowset.Rows may be enumerated only once. If user code needs to enumerate it multiple times, then all Rows must be cached during the first pass and use cached Rows later.");
            }

            m_enumeratorOutstanding = gcnew SqlIpRowsetEnumerator(this);

            // Do not return "this" to avoid object distruction by Dispose()
            return m_enumeratorOutstanding;
        }

        //
        // IEnumerable part
        //
        virtual IEnumerator^ GetEnumeratorIEnumerable() sealed = IEnumerable::GetEnumerator
        {
            return GetEnumerator();
        }

        virtual bool MoveNext() abstract;

        property IRow^ CurrentRow        
        {
            IRow^ get()
            {
                return m_currentRow;
            }
        }

        //
        // Amount of rows processed by the iterator
        //
        property unsigned __int64 RowCount
        {
            unsigned __int64 get()
            {
                if (m_enumeratorOutstanding != nullptr)
                {
                    return m_enumeratorOutstanding->RowCount;
                }

                return 0;
            }
        }

    }; // end of SqlIpRowset

    template<typename InputSchema>
    ref class SqlIpInputRowset : public SqlIpRowset
    {
        InputSchema                   &m_nativeRow;
        OperatorDelegate<InputSchema> *m_child;
        InteropAllocator              *m_allocator;
        
    public:
        
        SqlIpInputRowset(OperatorDelegate<InputSchema> * child,                                  
                         SqlIpSchema^ schema, 
                         cli::array<ColumnOffsetId>^ offsetId)
            : SqlIpRowset(schema, offsetId),
              m_child(child),
              m_nativeRow(*child->CurrentRowPtr())
        {
            m_child->ReloadCurrentRow();
        }

        InputSchema& GetCurrentNativeRow()
        {
            return m_nativeRow;
        }

        virtual bool MoveNext() override
        {
            if (!m_child->End())
            {
                bool hasRow = m_child->MoveNext();
                if (hasRow)
                {
                    //create a new row.
                    m_currentRow = gcnew SqlIpRow(m_schema, m_columnOffset, (unsigned char*)(&m_nativeRow));
                }
                
                return hasRow;
            }

            return false;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("SqlIpInputRowset");

            m_child->WriteRuntimeStats(node);
        }
        
    }; // end of SqlIpInputRowset

    //
    // Enumerate rowset honoring key ranges
    //
    template <typename InputSchema, typename KeyPolicy>
    ref class SqlIpInputKeyset : public SqlIpRowset
    {

    public:

        typedef KeyIterator<OperatorDelegate<InputSchema>, KeyPolicy> KeyIteratorType;
    
        SqlIpInputKeyset(KeyIteratorType * keyIterator,                                  
                         SqlIpSchema^ schema, 
                         cli::array<ColumnOffsetId>^ offsetId) 
            : SqlIpRowset(schema, offsetId),
              m_state(State::eINITIAL)
        {
            SCOPE_ASSERT(keyIterator != nullptr);
            
            m_iter = keyIterator;
        }

        ~SqlIpInputKeyset()
        {
            this->!SqlIpInputKeyset();
        }

        !SqlIpInputKeyset()
        {
            m_iter = nullptr;
        }
        
        bool Init()
        {
            SCOPE_ASSERT(m_state == State::eINITIAL);

            m_iter->ReadFirst();
            m_iter->ResetKey();

            m_state = m_iter->End() ? State::eEOF : State::eSTART;

            InitStartRow();

            return m_state == State::eSTART;
        }

        //
        // End current key range and start next one
        //
        bool NextKey()
        {
            SCOPE_ASSERT(m_state != State::eINITIAL);

            switch(m_state)
            {
            case State::eSTART:
            case State::eRANGE:
                m_iter->Drain();
                // Fallthrough
            case State::eEND:
                m_iter->ResetKey();
                ResetEnumerator();
                m_state = m_iter->End() ? State::eEOF : State::eSTART;
            }

            InitStartRow();
            return m_state == State::eSTART;
        }

        void SetIterator(KeyIteratorType *iter)
        {
            m_iter = iter;
            
            if (m_state != State::eSTART && m_state != State::eRANGE)
            {
                m_state = m_iter->End() ? State::eEND : State::eSTART;
                InitStartRow();
            }
        }
        
        virtual bool MoveNext() override
        {
            SCOPE_ASSERT(m_state != State::eINITIAL);

            switch(m_state)
            {
            case State::eSTART:
                // Row was already read
                m_state = State::eRANGE;
                break;

            case State::eRANGE:
                m_iter->Increment();
                m_state = m_iter->End() ? State::eEND : State::eRANGE;
                InitRangeRow();
                break;
            }

            return m_state == State::eRANGE;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("SqlIpInputKeyset");

            if (m_iter != nullptr)
            {
                m_iter->WriteRuntimeStats(node);
            }
        }

    private:

        void InitStartRow()
        {
            if (m_state == State::eSTART)
            {
                m_currentRow = gcnew SqlIpRow( m_schema, m_columnOffset, (unsigned char*)(m_iter->GetRow()) );            
            }        
        }

        void InitRangeRow()
        {
            if (m_state == State::eRANGE)
            {
                m_currentRow = gcnew SqlIpRow( m_schema, m_columnOffset, (unsigned char*)(m_iter->GetRow()) );            
            }        
        }
        
    private:

        enum class State { eINITIAL, eSTART, eRANGE, eEND, eEOF };
        State m_state;
        
        KeyIteratorType * m_iter; // does not own key iterator
        
    }; //end of SqlIpInputKeyset

    // SqlIpStreamProxy - user proxy around our internal stream
    //  This allows us to disable exposed functionality (read on output, write on input, closing/flushing of our stream, etc)
    template<typename T>
    ref class SqlIpStreamProxy abstract : public System::IO::Stream
    {
    protected:
        T       m_stream;
        bool    m_closed;
        
    public:
        SqlIpStreamProxy(T baseStream) 
            : m_stream(baseStream),
              m_closed(false)
        {
        }

        virtual property bool       CanSeek
        {
            bool get() override
            {
                return false;
            }
        }

        virtual property LONGLONG   Length
        {
            LONGLONG get() override
            {
                throw gcnew NotSupportedException();
            }
        }

        virtual property LONGLONG   Position
        {
            LONGLONG get() override
            {
                VerifyOpen();
                return m_stream->Position;
            }

            void set(LONGLONG pos) override
            {
                throw gcnew NotSupportedException();
            }
        }

        virtual LONGLONG            Seek(LONGLONG pos, System::IO::SeekOrigin origin) override
        {
            throw gcnew NotSupportedException();
        }

        virtual void                SetLength(LONGLONG length) override
        {
            throw gcnew NotSupportedException();
        }

        virtual void                Flush() override
        {
            //No-op
        }

        virtual void                Close() override
        {
            //No-op (to underlying stream)
            m_closed = true;
        }

        virtual void                VerifyOpen()
        {
            if(m_closed)
            {
                throw gcnew ObjectDisposedException("Cannot access a closed Stream.");
            }
        }
    };

    // SqlIpInputProxy
    template<typename T>
    ref class SqlIpInputProxy : public SqlIpStreamProxy<T>
    {
    public:
        SqlIpInputProxy(T baseStream) 
            : SqlIpStreamProxy<T>(baseStream)
        {
        }

        virtual property bool       CanRead
        {
            bool get() override
            {
                return !m_closed;
            }
        }

        virtual property bool       CanWrite
        {
            bool get() override
            {
                return false;
            }
        }

        virtual int                 Read(cli::array<System::Byte>^ buffer, int offset, int count) override
        {
            VerifyOpen();
            return m_stream->Read(buffer, offset, count);
        }

        virtual void                Write(cli::array<System::Byte>^ buffer, int offset, int count) override
        {
            throw gcnew NotSupportedException();
        }
    };

    // SqlIpOutputProxy
    template<typename T>
    ref class SqlIpOutputProxy : public SqlIpStreamProxy<T>
    {
    public:
        SqlIpOutputProxy(T baseStream) 
            : SqlIpStreamProxy<T>(baseStream)
        {
        }

        virtual property bool       CanRead
        {
            bool get() override
            {
                return false;
            }
        }

        virtual property bool       CanWrite
        {
            bool get() override
            {
                return !m_closed;
            }
        }

        virtual int                 Read(cli::array<System::Byte>^ buffer, int offset, int count) override
        {
            throw gcnew NotSupportedException();
        }

        virtual void                Write(cli::array<System::Byte>^ buffer, int offset, int count) override
        {
            VerifyOpen();
            m_stream->Write(buffer, offset, count);
        }
    };

    // SqlIpRowStream represents a SQLIP row in byte stream format, to enable extractor udo to process data in stream mode.
    //
    // We return a SqlIpInputProxy rather than the actual SqlIpRowStream stream so if the user gets a reference to it we can safely invalidate it
    // - the proxy contains a reference to a SqlIpRowStream which represents the real stream
    //   to verify that it is still valid
    //
    // SqlIpRowStream is based on another stream (ScopeCosmosStream, contains multiple rows) 
    // but it's aware row boundary. SqlIpRowStream::Read interface never move cursor cross row delimiter, 
    // it returns 0 when hiting row delimiter, simulating an EOF. 
    //
    // Idea is to setup a stage buffer, copy content from under layer stream into
    // stage buffer, search row delimiter, and return content when Read method being called. 
    // The stage buffer is used to avoid pushing row delimiter concept into under-layer stream,
    // and avoiding using user buffer for parsing delimiter.
    //
    // KMP algorithm is used for pattern search in stream. 
    //
    ref class SqlIpRowStream
    {
    public:
        // Get position in row stream
        property LONGLONG Position
        {
            LONGLONG get()
            {
                return m_posInRow;
            }
        }
        
        int Read(cli::array<System::Byte>^ dst, int offset, int count) 
        {   
            Validate(dst, offset, count);
            return this->ReadInternal(dst, offset, count, true/*copy bytes to dst*/);
        }

        // ctor    
        SqlIpRowStream(System::IO::Stream^ s, 
                       INT64 splitLength, 
                       cli::array<System::Byte>^ delimiter,
                       int bufferSize) 
            : m_posInRow(0), 
              m_stream(s), 
              m_delimiter(delimiter), 
              m_rowEnd(false),
              m_splitLength(splitLength),
              m_cb(0),
              x_bufSize(bufferSize)
        {
            // kmp table
            m_kmp = CreateKMPTable(m_delimiter);

            // acutal buffer size is 4MB + extra, extra is delimiter.Length - 1
            m_bufSize  = x_bufSize + m_delimiter->Length - 1;

            // create buffer
            m_buf = gcnew cli::array<System::Byte>(m_bufSize);

            // fill buffer
            m_start = 0;
            m_end = m_stream->Read(m_buf, 0, m_bufSize) + m_start;

            // search delimiter
            m_delimiterIdx = DelimiterMatch(m_start - 1, m_end);            
        }

        ~SqlIpRowStream()
        {
            delete m_buf;
            m_buf = nullptr;

            delete m_kmp;
            m_kmp = nullptr;

            m_stream = nullptr;
            m_delimiter = nullptr;
            
            this->!SqlIpRowStream();
        }
        
        property bool RowEnd
        {
            bool get() 
            {
                return m_rowEnd;
            }      
        }

        property bool IsEOF
        {
            bool get() 
            {
                // either 
                // 1. reach EOF for under-layer stream
                // 2. cursor passes split end point, and after that passes one complete delimiter
                return (m_start == m_end) || (m_cb >= m_splitLength + m_delimiter->Length); 
            }                  
        }

        // Start a new row and return a proxy for accessing it
        SqlIpInputProxy<SqlIpRowStream^>^ StartNewRow()
        {
            m_rowEnd = false;
            m_posInRow = 0;
            return gcnew SqlIpInputProxy<SqlIpRowStream^>(this);
        }
        
        void SkipCurRow()
        {
            this->ReadInternal(nullptr, 0, x_bufSize, false/*no copy*/);
        }

    protected:

        // read (or advance cursor) until any conditino below hit
        // 1) read bytes count as requested
        // 2) end of file
        // 3) end of row
        int ReadInternal(cli::array<System::Byte>^ dst, int offset, int count, bool copy) 
        {   
            // Only re-search delimiter in m_buf under some cases
            bool searchDelimiter = false;

            int read = 0;
            int remain = count;
            
            while (read < count)
            {
                // EOF, stop reading
                if (this->IsEOF)
                {                   
                    break;
                }

                // reach row end, stop reading
                if (m_rowEnd)
                {                    
                    break;
                }

                int c = 0;
                if (m_delimiterIdx == -1)
                {
                    // there is no delimiter in buffer. copy bytes out, but never cross the x_bufSize boundary
                    c = std::min(remain, std::min(m_end - m_start, x_bufSize - m_start));
                    if (copy) Array::Copy(m_buf, m_start, dst, offset, c);                  
                }
                else
                {
                    // there is delimiter in buffer
                    c = std::min(remain, m_delimiterIdx - m_start);
                    if (copy) Array::Copy(m_buf, m_start, dst, offset, c);
                    
                    // if m_start reaches delimiter, skip delimiter for preparation next read
                    if (c == m_delimiterIdx - m_start)
                    {
                        m_start += m_delimiter->Length;
                        m_rowEnd = true;        
                        m_cb += m_delimiter->Length;
                        searchDelimiter = true;
                    }                   
                }

                // update all counters
                offset += c;
                read += c; 
                m_start += c; 
                m_cb += c;
                remain -= c;
                m_posInRow += c;

                // if consumed bytes cross x_bufSize boundary, it's time to refill
                if (m_start >= x_bufSize)
                {
                    // move un-consumed bytes at buffer end, to buffer head. Array::Copy is safe even src/dst overlap           
                    int copy = m_end - m_start;                    
                    Array::Copy(m_buf, m_start, m_buf, 0, copy);

                    // refill                    
                    m_end = m_stream->Read(m_buf, copy, m_bufSize - copy) + copy;
                    m_start = 0;
                    searchDelimiter = true;
                }

                if (searchDelimiter)
                {
                    m_delimiterIdx = DelimiterMatch(m_start - 1, m_end);
                }
            }

            return read;
        }
        
        // Create table for KMP algorithm
        cli::array<int>^ CreateKMPTable(cli::array<System::Byte>^ pattern)
        {
            cli::array<int>^ t = gcnew cli::array<int>(pattern->Length);
            int j = 0;
            int k = -1;

            t[0] = -1;
            while (j < pattern->Length - 1)
            {
                if (k == -1 || pattern[j] == pattern[k])
                {
                    j++;
                    k++;
                    if (pattern[j] != pattern[k])
                    {
                        t[j] = k;
                    }
                    else
                    {
                        t[j] = t[k];
                    }
                }
                else
                {
                    k = t[k];
                }
            }

            return t;
        }

        // KMP pattern match
        // start is the starting idx - 1, end is the end idx in buffer. (start, end)
        int DelimiterMatch(int start, int end)
        {
            int i = start;
            int j = -1;
            while (i < end && j < m_delimiter->Length)
            {
                if (j == -1 || m_buf[i] == m_delimiter[j])
                {
                    i++; j++;
                }
                else
                {
                    j = m_kmp[j];
                }
            }

            if (j == m_delimiter->Length)
            {
                return i - m_delimiter->Length;
            }
            else
            {
                return -1;
            }
        }

        void Validate(cli::array<System::Byte>^ buffer, int offset, int count)
        {        
            if (buffer == nullptr)
            {
                throw gcnew ArgumentNullException("buffer", "buffer cannot be null");
            }
            if (offset + count > buffer->Length)
            {
                throw gcnew ArgumentException("offset and count were out of bounds for the array");
            }
            else if (offset < 0 || count < 0)
            {
                throw gcnew ArgumentOutOfRangeException("Non-negative number required");
            }            
        }
        
        !SqlIpRowStream()
        {
        }
        
    private:

        // position in row stream
        INT64 m_posInRow;

        // buffer size default is 4MB
        int x_bufSize;

        // stage buffer
        cli::array<System::Byte>^ m_buf;

        // actual buf size = x_bufSize + delimiter.Length - 1
        int m_bufSize; 

        // delimiter
        cli::array<System::Byte>^ m_delimiter;

        // NEXT table in kmp algorithm
        cli::array<int>^ m_kmp;

        // [start, end) idx guarding valide content in m_buf
        int m_start;
        int m_end;

        // index for matched delimiter in m_buf, -1 means no match
        int m_delimiterIdx;

        // flag indicate row is end, a.k.a. cursor passes row delimiter
        bool m_rowEnd;        

        // under layer stream
        System::IO::Stream^ m_stream;

        // stream split length
        INT64 m_splitLength;

        // total byte read since SqlIpRowStream created
        INT64 m_cb;
    }; // end of SqlIpRowStream

    ref class SqlIpRowEnumerator : public Generic::IEnumerator<System::IO::Stream^>
    {
    public:
        
        // IEnumerator interface
        virtual bool MoveNext()
        {
            // first time calling MoveNext
            if (m_row == nullptr)
            {
                m_row = gcnew SqlIpRowStream(m_stream, m_splitEnd - m_splitStart, m_delimiter, x_rowLimit);

                // not first split, skip first uncomplete row
                if (m_splitStart != 0)
                {
                    m_row->SkipCurRow();
                }
            }

            // For non-first row, we should check whether reader consume all bytes for previous row
            // if not, skip the uncompleted bytes before starting new row
            if (!m_row->RowEnd && m_rowCount > 0)
            {
                m_row->SkipCurRow();
            }

            // invalidate any outstanding proxy before exit
            if(m_proxy != nullptr)
            {
                m_proxy->Close();
            }
            
            if (m_row->IsEOF)
            {
                return false;
            }

            // start a new row
            m_proxy = m_row->StartNewRow();
            m_rowCount++;
            return true;
        }

        // IEnumerator interface
        property System::IO::Stream^ Current
        {
            virtual System::IO::Stream^ get()
            {
                if (m_row == nullptr || m_row->IsEOF)
                {
                    return nullptr;
                }

                // Limit the capability of streams passed to the user
                return m_proxy;
            }
        }

        // IEnumerator interface
        property Object^ CurrentBase
        {
            virtual System::Object^ get() sealed = IEnumerator::Current::get
            {
                return Current;
            }
        }
        
        // IEnumerator interface
        virtual void Reset()
        {
            throw gcnew NotSupportedException(); 
        }

        // ctor/dtor
        SqlIpRowEnumerator(System::IO::Stream^ s, INT64 splitStart, INT64 splitEnd, cli::array<System::Byte>^ delimiter)
            : m_stream(s), 
              m_splitStart(splitStart),
              m_splitEnd(splitEnd),
              m_delimiter(delimiter),
              m_rowCount(0),
              m_row(nullptr),
              m_proxy(nullptr)
        {
            SCOPE_ASSERT(m_stream->Position == 0);
            SCOPE_ASSERT(m_delimiter != nullptr);
            x_rowLimit = (int)Configuration::GetGlobal().GetMaxOnDiskRowSize();
        }

        ~SqlIpRowEnumerator()
        {
            delete m_proxy;
            m_proxy = nullptr;
            delete m_row;
            m_row = nullptr;

            m_delimiter = nullptr;
            m_stream = nullptr;
            
            this->!SqlIpRowEnumerator();
        }

    protected:

        !SqlIpRowEnumerator()
        {
        }
        
    private:

        // The underneath split stream
        System::IO::Stream^ m_stream;

        // split start/end offset
        INT64 m_splitStart;
        INT64 m_splitEnd;

        // row delimiter
        cli::array<System::Byte>^ m_delimiter;

        // stream represents a row
        SqlIpRowStream^ m_row;

        // Current row proxy - updated on each new row
        SqlIpInputProxy<SqlIpRowStream^>^ m_proxy;

        // row size limit 
        int x_rowLimit; 

        // how many rows processed
        int m_rowCount; 

    }; // end of SqlIpRowEnumerator
    
    ref class SqlIpRowReader : public Generic::IEnumerable<System::IO::Stream^>
    {
    public:
        
        // max delimiter length
        static const int x_delimiterMax = 32;

        // IEnumerable interface
        virtual Generic::IEnumerator<System::IO::Stream^>^ GetEnumerator() sealed = Generic::IEnumerable<System::IO::Stream^>::GetEnumerator
        {
            if (m_enumeratorOutstanding)
            {
                throw gcnew InvalidOperationException("User Error: Multiple instances of enumerators are not supported, the input stream may be enumerated only once.");
            }

            m_enumeratorOutstanding = true;
            return gcnew SqlIpRowEnumerator(m_stream, m_splitStart, m_splitEnd, m_delimiter);
        }

        // IEnumerable interface
        virtual IEnumerator^ GetEnumeratorBase() sealed = IEnumerable::GetEnumerator
        {
            return GetEnumerator();
        }

        // ctor/dtor
        SqlIpRowReader(System::IO::Stream^ s, INT64 splitStart, INT64 splitEnd, cli::array<System::Byte>^ delimiter)
            : m_stream(s), 
              m_splitStart(splitStart),
              m_splitEnd(splitEnd),
              m_delimiter(delimiter),
              m_enumeratorOutstanding(false)
        {
        }

        ~SqlIpRowReader()
        {
            m_delimiter = nullptr;
            m_stream = nullptr;
            
            this->!SqlIpRowReader();
        }

    protected:

        !SqlIpRowReader()
        {
        }
        
    private:

        // Enumeration singleton to prevent underlying concurrent stream access
        bool m_enumeratorOutstanding;

        // Saved parameters only to pass onto enumerator
        System::IO::Stream^         m_stream;
        INT64                       m_splitStart;
        INT64                       m_splitEnd;
        cli::array<System::Byte>^   m_delimiter;

    }; // end of SqlIpRowReader

    ref class SqlIpStreamReader : public IUnstructuredReader
    {
     
    public:   

        virtual property System::IO::Stream^ BaseStream
        {
            System::IO::Stream^ get() override
            {
                // Limit the capability of streams passed to the user
                return m_proxy;
            }
        }         

        virtual property LONGLONG Start
        {
            LONGLONG get() override
            {
                return m_splitStartOffset;
            }
        } 

        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                return m_splitEndOffset - m_splitStartOffset;
            }
        } 

        virtual Generic::IEnumerable<System::IO::Stream^>^ Split(cli::array<System::Byte>^ delimiter) override
        {
            // reuse the row reader
            if (m_rowReader != nullptr)
            {
                return m_rowReader;
            }

            SCOPE_ASSERT(m_stream);

            // When generating row reader, the base stream should be clean
            if (m_stream->Position != 0)
            {
                throw gcnew InvalidOperationException("BaseStream.Position should be zero");            
            }

            // Check delimiter parameter
            if (delimiter == nullptr || delimiter->Length == 0)
            {
                throw gcnew ArgumentException("delimiter can not be null or empty");
            }

            // Check delimiter length
            if (delimiter->Length > SqlIpRowReader::x_delimiterMax)
            {
                System::String^ err = System::String::Format("delimiter can not be longer than {0}", SqlIpRowReader::x_delimiterMax);                
                throw gcnew ArgumentException(err);
            }

            m_rowReader = gcnew SqlIpRowReader(m_stream, m_splitStartOffset, m_splitEndOffset, delimiter);
            return m_rowReader;            
        }
        
        SqlIpStreamReader(const InputFileInfo& input, SIZE_T bufSize, int bufCount)
        {
             m_stream = gcnew ScopeCosmosStream(input.inputFileName, true, bufSize, bufCount);
             m_stream->Init();
             m_proxy = gcnew SqlIpInputProxy<ScopeCosmosStream^>(m_stream);
             m_splitStartOffset = (INT64)input.splitStartOffset;
             m_splitEndOffset = (INT64)input.splitEndOffset;
             m_rowReader = nullptr;

             SCOPE_ASSERT(m_splitStartOffset >= 0);
             SCOPE_ASSERT(m_splitEndOffset >= 0);
             SCOPE_ASSERT(m_splitEndOffset >= m_splitStartOffset);             
        }             
        
        ~SqlIpStreamReader()
        {
            delete m_stream;
            m_stream = nullptr;

            delete m_proxy;
            m_proxy = nullptr;

            delete m_rowReader;
            m_rowReader = nullptr;
            
            this->!SqlIpStreamReader();
        }
        
        void Close()
        {
            m_stream->Close();
        }

        __int64 GetInclusiveTimeMillisecond()
        {
            return m_stream->GetInclusiveTimeMillisecond();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_stream->WriteRuntimeStats(root);
        }         
    
     protected:
    
         !SqlIpStreamReader()
         {
         }
         
     private:
     
         ScopeCosmosStream^ m_stream;        
         SqlIpInputProxy<ScopeCosmosStream^>^ m_proxy;        
         INT64 m_splitStartOffset;
         INT64 m_splitEndOffset;
         SqlIpRowReader^ m_rowReader;
     };

    ref class SqlIpStreamWriter : public IUnstructuredWriter
    {

    public:    

        virtual property System::IO::Stream^ BaseStream
        {
            System::IO::Stream^ get() override
            {
                if(m_proxy == nullptr)
                {
                    // Limit the capability of streams passed to the user
                    m_proxy = gcnew SqlIpOutputProxy<ScopeCosmosStream^>(m_stream);
                }

                return m_proxy;
            }
        }
        
        SqlIpStreamWriter(std::string& name, SIZE_T bufSize, int bufCount)
            : m_proxy(nullptr)
        {
            m_stream = gcnew ScopeCosmosStream(name, false, bufSize, bufCount);
            m_stream->Init();
        }        
        
        ~SqlIpStreamWriter()
        {
            delete m_stream;
            m_stream = nullptr;

            delete m_proxy;
            m_proxy = nullptr;

            this->!SqlIpStreamWriter();
        }
        
        void Close()
        {
            m_stream->Close();
        }

        void EndOfRow()
        {
            m_stream->Flush();

            // invalidate any outstanding proxy
            if(m_proxy != nullptr)
                m_proxy->Close();
            m_proxy = nullptr;
        }

        __int64 GetInclusiveTimeMillisecond()
        {
            return m_stream->GetInclusiveTimeMillisecond();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_stream->WriteRuntimeStats(root);
        }         
    
     protected:
    
         !SqlIpStreamWriter()
         {
         }
         
     private:
     
         ScopeCosmosStream^ m_stream;         
         SqlIpOutputProxy<ScopeCosmosStream^>^ m_proxy;
     };

    static INLINE void GetReadOnlyColumnOrdinal(ISchema^ inputSchema, 
                                                ISchema^ outputSchema,
                                                cli::array<String^>^ readOnlyColumns,
                                                cli::array<int>^ %inputOrdinal,  //out
                                                cli::array<int>^ %outputOrdinal  //out
                                                )
    {
        Generic::List<String^>^ match = gcnew Generic::List<String^>();
        for each(auto col in inputSchema)
        {
            if (Array::BinarySearch(readOnlyColumns, col->Name) >= 0)
            {
                match->Add(col->Name);
            }
        }
        
        int c = match->Count;
        if (c == 0)
        {
            return;
        }
        
        inputOrdinal = gcnew cli::array<int>(c);
        outputOrdinal = gcnew cli::array<int>(c);
                
        for(int idx = 0; idx < c; ++idx)
        {
            inputOrdinal[idx] = inputSchema->IndexOf(match[idx]);
            outputOrdinal[idx] = outputSchema->IndexOf(match[idx]);
        }                
    }

    template<typename OutputSchema, int UID, int RunScopeCEPMode>
    INLINE ScopeExtractorManaged<OutputSchema> * ScopeExtractorManagedFactory::MakeSqlIp(std::string * argv, int argc)
    {
        ManagedUDO<UID> managedUDO(argv, argc);
        ManagedRow<OutputSchema> managedRow;
        SqlIpSchema^ schema = gcnew SqlIpSchema(managedRow.Columns(nullptr), &ManagedRow<OutputSchema>::ComplexColumnGetter);
        SqlIpUpdatableRow^ outputRow = gcnew SqlIpUpdatableRow(schema->Defaults);
        
        return new SqlIpExtractor<OutputSchema>(outputRow,
                                                managedUDO.get(),
                                                managedUDO.args(),
                                                &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal);
    }

    template<typename OutputSchema>
    class SqlIpExtractor : public ScopeExtractorManaged<OutputSchema>
    {
        friend struct ScopeExtractorManagedFactory;

        static const char* const sm_className;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToNativeCallType)(IRow ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>              m_enumerator;
        ScopeTypedManagedHandle<IExtractor ^>                               m_extractor;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        ScopeTypedManagedHandle<SqlIpStreamReader ^>                        m_inputStreamReader;
        MarshalToNativeCallType                                             m_marshalToNative;
        IncrementalAllocator                                                m_allocator;
        ScopeTypedManagedHandle<SqlIpUpdatableRow ^>                        m_updatableRow;

        SqlIpExtractor(SqlIpUpdatableRow^ outputRow,
                       IExtractor ^ extractor, 
                       Generic::List<String^>^ args, 
                       MarshalToNativeCallType m)
            : m_extractor(extractor),
              m_args(args),
              m_marshalToNative(m),
              m_updatableRow(outputRow),
              m_allocator()
        {
        }

    public:
        virtual void CreateInstance(const InputFileInfo& input, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize)
        {
            m_inputStreamReader = gcnew SqlIpStreamReader(input, bufSize, bufCount);
            m_allocator.Init(virtualMemSize, sm_className);
        }

        virtual void Init()
        {
            m_enumerator = m_extractor->Extract(m_inputStreamReader.get(), m_updatableRow)->GetEnumerator();
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<IRow^> ^ enumerator = m_enumerator;
            
            if (enumerator->MoveNext())
            {
                m_allocator.Reset();
                CheckSqlIpUdoYieldRow(enumerator->Current, (System::Object^)m_updatableRow.get());
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();

            // Dispose enumerator
            m_enumerator.reset();
            m_inputStreamReader->Close();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputStreamReader)
            {
                m_inputStreamReader->WriteRuntimeStats(node);
            }
        }

        virtual ~SqlIpExtractor()
        {
            // There only to ensure proper destruction when base class destructor is called
            if (m_inputStreamReader)
            {
                try
                {
                    m_inputStreamReader.reset();
                }
                catch (std::exception&)
                {
                    // ignore any I/O errors as we are destroying the object anyway
                }
            }
        }

        // Time spend in reading
        virtual __int64 GetIOTime()
        {
            if (m_inputStreamReader)
            {
                return m_inputStreamReader->GetInclusiveTimeMillisecond();
            }

            return 0;
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpExtractor!");        
        }

        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpExtractor!");        
        }
    };

    template<typename OutputSchema>
    const char* const SqlIpExtractor<OutputSchema>::sm_className = "SqlIpExtractor";

    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeProcessorManagedFactory::MakeSqlIp(OperatorDelegate<InputSchema> * child)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        cli::array<System::String^> ^ readOnlyColumns = managedUDO.ReadOnlyColumns();
                
        ManagedRow<InputSchema> managedInputRow;
        SqlIpSchema^ inputSchema = gcnew SqlIpSchema(managedInputRow.Columns(readOnlyColumns), &ManagedRow<InputSchema>::ComplexColumnGetter);
        SqlIpInputRowset<InputSchema>^ inputRowset = gcnew SqlIpInputRowset<InputSchema>(child, inputSchema, managedInputRow.ColumnOffsets());
        
        ManagedRow<OutputSchema> managedOuputRow;
        SqlIpSchema^ outputSchema = gcnew SqlIpSchema(managedOuputRow.Columns(readOnlyColumns), &ManagedRow<OutputSchema>::ComplexColumnGetter);
        SqlIpUpdatableRow^ outputRow = gcnew SqlIpUpdatableRow(outputSchema->Defaults); 

        return new SqlIpProcessor<InputSchema, OutputSchema>(managedUDO.get(),
                                                             inputRowset,
                                                             outputRow,
                                                             &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal,
                                                             &RowTransformPolicy<InputSchema,OutputSchema,UID>::FilterTransformRow,
                                                             readOnlyColumns);
    }

    template<typename InputSchema, typename OutputSchema>
    class SqlIpProcessor : public ScopeProcessorManaged<InputSchema, OutputSchema>
    {
        friend struct ScopeProcessorManagedFactory;

        static const char* const sm_className;

        typedef void (*MarshalToNativeCallType)(IRow ^, OutputSchema &, IncrementalAllocator *);
        typedef bool (*TransformRowCallType)(InputSchema &, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<SqlIpInputRowset<InputSchema> ^>        m_inputRowset;
        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>          m_inputEnumerator;
        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>          m_outputEnumerator;
        ScopeTypedManagedHandle<IProcessor ^>                           m_processor;
        ScopeTypedManagedHandle<SqlIpUpdatableRow ^>                    m_outputRow;
        MarshalToNativeCallType                                         m_marshalToNative;
        TransformRowCallType                                            m_transformRowCall;
        RowEntityAllocator                                              m_allocator;
        ScopeTypedManagedHandle<cli::array<int> ^>                      m_inputReadOnlyColumnOrdinal;
        ScopeTypedManagedHandle<cli::array<int> ^>                      m_outputReadOnlyColumnOrdinal;        
        bool                                                            m_checkReadOnly;

        SqlIpProcessor(IProcessor ^ udo, 
                       SqlIpInputRowset<InputSchema>^ inputRowset, 
                       SqlIpUpdatableRow^ outputRow, 
                       MarshalToNativeCallType marshalToNativeCall, 
                       TransformRowCallType transformRowCall,
                       cli::array<System::String^>^ readOnlyColumns)
            : m_processor(udo),
              m_inputRowset(inputRowset),
              m_inputEnumerator(inputRowset->GetEnumerator()),
              m_outputRow(outputRow),
              m_marshalToNative(marshalToNativeCall),
              m_transformRowCall(transformRowCall),
              m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
              m_checkReadOnly(false)              
        {
            if (readOnlyColumns != nullptr && readOnlyColumns->Length > 0)
            {
                m_checkReadOnly = true;

                cli::array<int>^ inputOrdinal = nullptr;
                cli::array<int>^ outputOrdinal = nullptr; 
                GetReadOnlyColumnOrdinal(m_inputRowset->Schema, m_outputRow->Schema, readOnlyColumns, inputOrdinal, outputOrdinal);

                m_inputReadOnlyColumnOrdinal.reset(inputOrdinal);
                m_outputReadOnlyColumnOrdinal.reset(outputOrdinal);  
            }          
        }

        void ResetOutput()
        {
            // clear output row before handing over to udo        
            m_outputRow->Reset();

            // some columns are pass-through, copy (from input to output) to be visible in outputrow, inside udo            
            if (m_checkReadOnly)
            {                           
                m_outputRow->CopyColumns(m_inputEnumerator->Current, m_inputReadOnlyColumnOrdinal, m_outputReadOnlyColumnOrdinal);  
            }           
        }

    public:
    
        virtual void Init()
        {
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            if (m_outputEnumerator.get() != nullptr && m_outputEnumerator->MoveNext())
            {
                m_allocator.Reset();
                CheckSqlIpUdoYieldRow(m_outputEnumerator->Current, (System::Object^)m_outputRow.get());
                (*m_marshalToNative)(m_outputEnumerator->Current, output, &m_allocator);
                (*m_transformRowCall)(m_inputRowset->GetCurrentNativeRow(), output, nullptr);

                return true;
            }

            while (m_inputEnumerator->MoveNext())
            {
                ResetOutput();
                
                m_outputEnumerator.reset( m_processor->Process(m_inputEnumerator->Current, m_outputRow.get())->GetEnumerator() );

                if (m_outputEnumerator->MoveNext())
                {
                    m_allocator.Reset();
                    CheckSqlIpUdoYieldRow(m_outputEnumerator->Current, (System::Object^)m_outputRow.get());
                    (*m_marshalToNative)(m_outputEnumerator->Current, output, &m_allocator);
                    (*m_transformRowCall)(m_inputRowset->GetCurrentNativeRow(), output, nullptr);

                    return true;
                }
            }

            return false;
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_inputEnumerator.reset();
            m_outputEnumerator.reset();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputRowset)
            {
                m_inputRowset->WriteRuntimeStats(node);
            }
        }

        virtual ~SqlIpProcessor()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpProcessor!");        
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpProcessor!");        
        }
    };

    template<typename InputSchema, typename OutputSchema>
    const char* const SqlIpProcessor<InputSchema, OutputSchema>::sm_className = "SqlIpProcessor";

    template<typename InputSchema, int UID, int RunScopeCEPMode>
    ScopeOutputerManaged<InputSchema> * ScopeOutputerManagedFactory::MakeSqlIp(OperatorDelegate<InputSchema> * child)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        ManagedRow<InputSchema> managedRow;        

        SqlIpSchema^ schema = gcnew SqlIpSchema(managedRow.Columns(nullptr), &ManagedRow<InputSchema>::ComplexColumnGetter);
        SqlIpInputRowset<InputSchema>^ rowset = gcnew SqlIpInputRowset<InputSchema>(child, schema, managedRow.ColumnOffsets());
        return new SqlIpOutputer<InputSchema>(rowset, managedUDO.get());
    }

    template<typename InputSchema>
    class SqlIpOutputer : public ScopeOutputerManaged<InputSchema>
    {
        friend struct ScopeOutputerManagedFactory;
        static const char* const sm_className;

        ScopeTypedManagedHandle<IOutputter ^>                       m_outputer;
        ScopeTypedManagedHandle<SqlIpInputRowset<InputSchema> ^>    m_inputRowset;
        ScopeTypedManagedHandle<SqlIpStreamWriter ^>                m_outputStreamWriter;

        SqlIpOutputer(SqlIpInputRowset<InputSchema>^ rowset, IOutputter ^ outputer)
        {
            m_outputer = outputer;
            m_inputRowset = rowset;
        }

    public:
        virtual ~SqlIpOutputer()
        {
            // There only to ensure proper destruction when base class destructor is called
            if (m_outputStreamWriter)
            {
                try
                {
                    m_outputStreamWriter.reset();
                }
                catch (std::exception&)
                {
                    // ignore any I/O errors as we are destroying the object anyway
                }
            }
        }

        virtual void CreateStream(std::string& outputName, SIZE_T bufSize, int bufCnt)
        {
            m_outputStreamWriter = gcnew SqlIpStreamWriter(outputName, bufSize, bufCnt);
        }

        virtual void Init()
        {
        }

        virtual void Output()
        {
            auto writer = m_outputStreamWriter.get();
            for each(auto row in m_inputRowset.get())
            {                
                // UDO::Output
                m_outputer->Output(row, writer);

                // EOR
                //  Flushes and invalidates any outstanding proxy
                writer->EndOfRow();
            }
        }

        virtual void Close()
        {
            if (m_outputStreamWriter)
            {
                m_outputStreamWriter->Close();
            }
        }

        virtual __int64 GetIOTime()
        {
            if (m_outputStreamWriter)
            {
                return m_outputStreamWriter->GetInclusiveTimeMillisecond();
            }

            return 0;
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            RuntimeStats::WriteRowCount(root, (LONGLONG)m_inputRowset->RowCount);

            auto & node = root.AddElement(sm_className);

            if (m_inputRowset)
            {
                m_inputRowset->WriteRuntimeStats(node);
            }

            if (m_outputStreamWriter)
            {
                m_outputStreamWriter->WriteRuntimeStats(node);
            }
        }
        
        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpOutputer!");        
        }
        
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpOutputer!");          
        }
    };// end of SqlIpOutputer

    template<typename InputSchema>
    const char* const SqlIpOutputer<InputSchema>::sm_className = "SqlIpOutputer";

    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeReducerManagedFactory::MakeSqlIp(OperatorDelegate<InputSchema> * child)
    {
        typedef KeyComparePolicy<InputSchema, UID> KeyPolicy;
        ManagedUDO<UID> managedUDO(nullptr, 0);
        cli::array<System::String^> ^ readOnlyColumns = managedUDO.ReadOnlyColumns();
                
        ManagedRow<InputSchema> inputRow;        
        SqlIpSchema^ inputSchema = gcnew SqlIpSchema(inputRow.Columns(readOnlyColumns), &ManagedRow<InputSchema>::ComplexColumnGetter);

        ManagedRow<OutputSchema> outputRow;
        SqlIpSchema^ outputSchema = gcnew SqlIpSchema(outputRow.Columns(readOnlyColumns),&ManagedRow<OutputSchema>::ComplexColumnGetter);
        SqlIpUpdatableRow^ updatableRow = gcnew SqlIpUpdatableRow(outputSchema->Defaults);
        
        return new SqlIpReducer<InputSchema, OutputSchema, KeyPolicy>(child,
                                                                      inputSchema,
                                                                      inputRow.ColumnOffsets(),
                                                                      updatableRow,
                                                                      managedUDO.get(),
                                                                      &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal,
                                                                      readOnlyColumns);
    }

    template<typename InputSchema, typename OutputSchema, typename KeyPolicy>
    class SqlIpReducer : public ScopeProcessorManaged<InputSchema, OutputSchema>
    {
        friend struct ScopeReducerManagedFactory;

        static const char* const sm_className;

        InputSchema m_inputRow;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToNativeCallType)(IRow ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>              m_enumerator;       
        ScopeTypedManagedHandle<SqlIpInputKeyset<InputSchema,KeyPolicy> ^>  m_inputKeyset;
        ScopeTypedManagedHandle<SqlIpUpdatableRow ^>                        m_outputRow;        
        ScopeTypedManagedHandle<IReducer ^>                                 m_reducer;
        MarshalToNativeCallType                                             m_marshalToNative;
        RowEntityAllocator                                                  m_allocator;
        bool                                                                m_hasMoreRows;
        bool                                                                m_checkReadOnly;
        ScopeTypedManagedHandle<cli::array<int> ^>                          m_inputReadOnlyColumnOrdinal;
        ScopeTypedManagedHandle<cli::array<int> ^>                          m_outputReadOnlyColumnOrdinal;        

        typename SqlIpInputKeyset<InputSchema,KeyPolicy>::KeyIteratorType   m_keyIterator;
    
        SqlIpReducer(OperatorDelegate<InputSchema> *          child,
                     SqlIpSchema^                             inputSchema,
                     cli::array<ScopeEngine::ColumnOffsetId>^ columnOffset,
                     SqlIpUpdatableRow ^                      outputRow,
                     IReducer ^                               udo, 
                     MarshalToNativeCallType                  marshalToNativeCall,
                     cli::array<String^>^                     readOnlyColumns)
            : m_keyIterator(child),
              m_outputRow(outputRow),
              m_reducer(udo),
              m_marshalToNative(marshalToNativeCall),
              m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
              m_hasMoreRows(false),
              m_checkReadOnly(false)
        {
            m_inputKeyset.reset(gcnew SqlIpInputKeyset<InputSchema,KeyPolicy>(&m_keyIterator, inputSchema, columnOffset));

            if (readOnlyColumns != nullptr && readOnlyColumns->Length > 0)
            {
                m_checkReadOnly = true;

                cli::array<int>^ inputOrdinal = nullptr;
                cli::array<int>^ outputOrdinal = nullptr; 
                GetReadOnlyColumnOrdinal(m_inputKeyset->Schema, m_outputRow->Schema, readOnlyColumns, inputOrdinal, outputOrdinal);

                m_inputReadOnlyColumnOrdinal.reset(inputOrdinal);
                m_outputReadOnlyColumnOrdinal.reset(outputOrdinal);  
            }                
        }

        void ResetOutput()
        {
            // clear output row before handing over to udo        
            m_outputRow->Reset();

            // some columns are pass-through, copy (from input to output) to be visible in outputrow, inside udo            
            if (m_checkReadOnly)
            {                
                m_outputRow->CopyColumns(m_inputKeyset->CurrentRow, m_inputReadOnlyColumnOrdinal, m_outputReadOnlyColumnOrdinal);  
            }           
        }
        
    public:
    
        virtual void Init()
        {
            m_hasMoreRows = m_inputKeyset->Init();

            ResetOutput();
            
            if (m_hasMoreRows)
            {
                m_enumerator = m_reducer->Reduce(m_inputKeyset, m_outputRow)->GetEnumerator();
            }
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            while(m_hasMoreRows)
            {
                Generic::IEnumerator<IRow^> ^ enumerator = m_enumerator;                

                if (enumerator->MoveNext())
                {
                    m_allocator.Reset();
                    CheckSqlIpUdoYieldRow(enumerator->Current, (System::Object^)m_outputRow.get());
                    (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                    return true;
                }
                else if (m_inputKeyset->NextKey())
                {                    
                    ResetOutput();
                    
                    // Proceed to the next key and create enumerator for it
                    m_enumerator.reset(m_reducer->Reduce(m_inputKeyset, m_outputRow)->GetEnumerator());
                }
                else
                {
                    // EOF
                    m_hasMoreRows = false;
                }
            }

            return false;
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputKeyset)
            {
                m_inputKeyset->WriteRuntimeStats(node);
            }
        }

        virtual ~SqlIpReducer()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpReducer!");        
        }
        
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpReducer!");          
        }
    };

    template<typename InputSchema, typename OutputSchema, typename KeyPolicy>
    const char* const SqlIpReducer<InputSchema, OutputSchema, KeyPolicy>::sm_className = "SqlIpReducer";


    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy, int UID>
    INLINE SqlIpCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy> * SqlIpCombinerManagedFactory::MakeSqlIp(OperatorDelegate<InputSchemaLeft> * leftChild,
                                                                                                                                                         OperatorDelegate<InputSchemaRight> * rightChild)
    {
        // udo
        ManagedUDO<UID> managedUDO(nullptr, 0);        
        cli::array<System::String^> ^ readOnlyColumns = managedUDO.ReadOnlyColumns();
        
        // output row
        ManagedRow<OutputSchema> outputRow;        
        SqlIpSchema^ outputSchema = gcnew SqlIpSchema(outputRow.Columns(readOnlyColumns), &ManagedRow<OutputSchema>::ComplexColumnGetter);
        SqlIpUpdatableRow^ updatableRow = gcnew SqlIpUpdatableRow(outputSchema->Defaults);

        // Create combiner
        return new SqlIpCombiner<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy>(managedUDO.get(),
                                                                                                                 leftChild,
                                                                                                                 rightChild,
                                                                                                                 updatableRow,
                                                                                                                 &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal,
                                                                                                                 readOnlyColumns);
        
    }

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy>
    class SqlIpCombiner : public SqlIpCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy>
    {
        friend struct SqlIpCombinerManagedFactory;

        static const char* const sm_className;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToNativeCallType)(IRow ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>                         m_enumerator;
        ScopeTypedManagedHandle<SqlIpInputKeyset<InputSchemaLeft, LeftKeyPolicy> ^>    m_inputRowsetLeft;
        ScopeTypedManagedHandle<SqlIpInputKeyset<InputSchemaRight, RightKeyPolicy> ^>  m_inputRowsetRight;
        ScopeTypedManagedHandle<SqlIpUpdatableRow^>                                    m_outputRow;
        ScopeTypedManagedHandle<ICombiner ^>                                           m_combiner;
        MarshalToNativeCallType                                                        m_marshalToNative;
        RowEntityAllocator                                                             m_allocator;

        LeftKeyIteratorType                                                            m_leftKeyIterator;
        RightKeyIteratorType                                                           m_rightKeyIterator;        
        ScopeTypedManagedHandle<SqlIpSchema^>                                          m_leftSchema;
        ScopeTypedManagedHandle<SqlIpSchema^>                                          m_rightSchema;
        ScopeTypedManagedHandle<cli::array<ScopeEngine::ColumnOffsetId>^>              m_leftColumnOffset;
        ScopeTypedManagedHandle<cli::array<ScopeEngine::ColumnOffsetId>^>              m_rightColumnOffset;   

        bool                                                                           m_firstRowInKeySet;
        bool                                                                           m_checkReadOnly;
        ScopeTypedManagedHandle<cli::array<int> ^>                                     m_inputReadOnlyColumnOrdinalLeft;
        ScopeTypedManagedHandle<cli::array<int> ^>                                     m_inputReadOnlyColumnOrdinalRight;
        ScopeTypedManagedHandle<cli::array<int> ^>                                     m_outputReadOnlyColumnOrdinalLeft;     
        ScopeTypedManagedHandle<cli::array<int> ^>                                     m_outputReadOnlyColumnOrdinalRight;             

        SqlIpCombiner(ICombiner ^                                         udo,
                      OperatorDelegate<InputSchemaLeft> *                 leftChild,
                      OperatorDelegate<InputSchemaRight>*                 rightChild,
                      SqlIpUpdatableRow^                                  outputRow,
                      MarshalToNativeCallType                             marshalToNativeCall,
                      cli::array<String^>^                                readOnlyColumns)        
            : m_combiner(udo), 
              m_leftKeyIterator(leftChild),
              m_rightKeyIterator(rightChild),
              m_outputRow(outputRow),
              m_marshalToNative(marshalToNativeCall), 
              m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
              m_checkReadOnly(false),
              m_firstRowInKeySet(false)
        {
            // left input rowset
            ManagedRow<InputSchemaLeft> leftRow;        
            m_leftSchema.reset(gcnew SqlIpSchema(leftRow.Columns(readOnlyColumns), &ManagedRow<InputSchemaLeft>::ComplexColumnGetter));
            m_leftColumnOffset.reset(leftRow.ColumnOffsets());
            
            // right input rowset
            ManagedRow<InputSchemaRight> rightRow;        
            m_rightSchema.reset(gcnew SqlIpSchema(rightRow.Columns(readOnlyColumns), &ManagedRow<InputSchemaRight>::ComplexColumnGetter));
            m_rightColumnOffset.reset(rightRow.ColumnOffsets());

            if (readOnlyColumns != nullptr && readOnlyColumns->Length > 0)
            {
                m_checkReadOnly = true;

                cli::array<int>^ inputOrdinal = nullptr;
                cli::array<int>^ outputOrdinal = nullptr; 
                GetReadOnlyColumnOrdinal(m_leftSchema, m_outputRow->Schema, readOnlyColumns, inputOrdinal, outputOrdinal);
                m_inputReadOnlyColumnOrdinalLeft.reset(inputOrdinal);
                m_outputReadOnlyColumnOrdinalLeft.reset(outputOrdinal);

                GetReadOnlyColumnOrdinal(m_rightSchema, m_outputRow->Schema, readOnlyColumns, inputOrdinal, outputOrdinal);
                m_inputReadOnlyColumnOrdinalRight.reset(inputOrdinal);
                m_outputReadOnlyColumnOrdinalRight.reset(outputOrdinal);
            }              
        }

        void ResetOutput()
        {
            // clear output row before handing over to udo        
            m_outputRow->Reset();

            // some columns are pass-through, copy (from input to output) to be visible in outputrow, inside udo            
            if (m_checkReadOnly)
            {                
                if (m_inputReadOnlyColumnOrdinalLeft != nullptr)
                {
                    m_outputRow->CopyColumns(m_inputRowsetLeft->CurrentRow, m_inputReadOnlyColumnOrdinalLeft, m_outputReadOnlyColumnOrdinalLeft);  
                }

                if (m_inputReadOnlyColumnOrdinalRight != nullptr)
                {
                    m_outputRow->CopyColumns(m_inputRowsetRight->CurrentRow, m_inputReadOnlyColumnOrdinalRight, m_outputReadOnlyColumnOrdinalRight);                  
                }
            }           
        }
        
    public:
        virtual void Init()
        {
            m_inputRowsetLeft.reset(gcnew SqlIpInputKeyset<InputSchemaLeft, LeftKeyPolicy>(&m_leftKeyIterator, m_leftSchema, m_leftColumnOffset));        
            m_inputRowsetRight.reset(gcnew SqlIpInputKeyset<InputSchemaRight, RightKeyPolicy>(&m_rightKeyIterator, m_rightSchema, m_rightColumnOffset));
            
            m_enumerator.reset(m_combiner->Combine(m_inputRowsetLeft, m_inputRowsetRight, m_outputRow)->GetEnumerator()); 
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<IRow^> ^ enumerator = m_enumerator;

            // When calling into Udo first time, call ResetOutput to copy read only columns
            if (!m_firstRowInKeySet)
            {
                ResetOutput();
                m_firstRowInKeySet = true;
            }
                        
            if (enumerator->MoveNext())
            {
                m_allocator.Reset();
                CheckSqlIpUdoYieldRow(enumerator->Current, (System::Object^)m_outputRow.get());
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);
                
                return true;
            }
            else
            {   
                m_firstRowInKeySet = false;                
                Init();
                return false;
            }
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual void SetLeftKeyIterator(LeftKeyIteratorType* iter)
        {
            m_inputRowsetLeft->SetIterator(iter);
        }
        
        virtual void SetRightKeyIterator(RightKeyIteratorType* iter)
        {
            m_inputRowsetRight->SetIterator(iter);        
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputRowsetLeft)
            {
                m_inputRowsetLeft->WriteRuntimeStats(node);
            }
            if (m_inputRowsetRight)
            {
                m_inputRowsetRight->WriteRuntimeStats(node);
            }
        }

        virtual ~SqlIpCombiner()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpCombiner!");        
        }
        
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpCombiner!");          
        }
    };

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy>
    const char* const SqlIpCombiner<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy>::sm_className = "SqlIpCombiner";

#pragma endregion SQLIP
} // namespace ScopeEngine
