cdef class CopyReader:

    cdef object copyobj
    cdef object iterator
    cdef object bufferobj
    cdef bint closed

    cpdef bytes read(self, long long size)
    cpdef void close(self)
