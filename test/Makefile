###################################################
# Source and target file
###################################################
target_list 	= TestAllPlacesIncrementAllKeys

target		= TestAllPlacesIncrementAllKeys

# build path
build_path	= build

# compile options
X10_FLAG	= -sourcepath $(X10_HOME)/x10.tests/tests/x10lib:../src -d $(build_path)  -errors 5 -VERBOSE_CHECKS 

X10CXX = x10c++

X10RTIMPL = sockets
###################################################
# Build
###################################################
default	:	 
	$(X10CXX) -g -x10rt $(X10RTIMPL) $(X10_FLAG) $(target).x10 -o $@

run		: run_java
###################################################


