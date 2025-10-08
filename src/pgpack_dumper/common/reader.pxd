cdef class CopyReader:

    cdef object copyobj
    cdef object iterator
    cdef object bufferobj
    cdef bint closed
    cdef long long total_read

    cpdef bytes read(self, long long size)
    cpdef long long tell(self)
    cpdef void close(self)
