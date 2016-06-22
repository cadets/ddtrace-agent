all:
	cc \
	-D NEED_SOLARIS_BOOLEAN \
	-I /usr/local/include \
	-L /usr/local/lib \
	-I /usr/src/contrib/libxo \
	-I /usr/src/cddl/compat/opensolaris/include \
	-I /usr/src/cddl/contrib/opensolaris/lib/libdtrace/common \
	-I /usr/src/cddl/contrib/opensolaris/lib/libctf/common \
	-I /usr/src/cddl/lib/libdtrace \
	-I /usr/src/sys/cddl/compat/opensolaris \
	-I /usr/src/sys/cddl/contrib/opensolaris/uts/common \
	ddtrace_agent.c \
        ddtrace.c \
	-l dtrace -l proc -l ctf -l elf -l z -l rtld_db -l pthread -l util \
	-l xo -lcrypto -lssl -l rdkafka \
	-o ddtrace_agent -Wl,-rpath,/usr/local/lib 
