cdef class CopyReader:

    cdef public object copyobj
    cdef public object iterator
    cdef public bytearray bufferobj
    cdef public bytes first_data
    cdef public bint closed
    cdef public long long total_read

    cpdef bytes read(self, long long size)
    cpdef long long tell(self)
    cpdef void close(self)
