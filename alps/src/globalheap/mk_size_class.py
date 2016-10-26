# 
# (c) Copyright 2016 Hewlett Packard Enterprise Development LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

def make_size_class_cc(size_class_ranges):
    size_classes = []

    for r in size_class_ranges:
        size_classes.extend(range(r[0], r[1], r[2]))

    size_class_str = ['%dLLU' % size_class for size_class in size_classes]
    size_class_str =  '{' + ', '.join(size_class_str) + '}'

    lines = []
    lines.append('#include "globalheap/size_class.hh"\n')
    lines.append('\n')
    lines.append('namespace alps {\n')
    lines.append('\n')
    lines.append('size_t size_table[kSizeClasses] = ' + size_class_str + ';\n')
    lines.append('\n')
    lines.append('} // namespace alps')

    return lines

def make_size_class_hh(size_class_ranges):
    size_classes = []

    for r in size_class_ranges:
        size_classes.extend(range(r[0], r[1], r[2]))


    lines = []
    lines.append('#ifndef _ALPS_GLOBALHEAP_SIZE_CLASS_HH_\n')
    lines.append('#define _ALPS_GLOBALHEAP_SIZE_CLASS_HH_\n')
    lines.append('\n')
    lines.append('#include <sys/types.h>\n')
    lines.append('\n')
    lines.append('namespace alps {\n')
    lines.append('\n')
    lines.append('const int kSizeClasses = %s;\n' % len(size_classes))
    lines.append('\n')
    lines.append('extern size_t size_table[kSizeClasses];\n')
    lines.append('\n')
    lines.append('inline int sizeclass(size_t sz)\n')
    lines.append('{\n')
    lines.append('    int sizeclass = 0;\n')
    lines.append('    while (size_table[sizeclass] < sz) {\n')
    lines.append('        sizeclass++;\n')
    lines.append('    }\n')
    lines.append('    return sizeclass;\n')
    lines.append('}\n')
    lines.append('\n')
    lines.append('inline size_t size_from_class(const int sizeclass)\n')
    lines.append('{\n')
    lines.append('    return size_table[sizeclass];\n')
    lines.append('}\n')
    lines.append('\n')
    lines.append('} // namespace alps\n')
    lines.append('\n')
    lines.append('#endif // _ALPS_GLOBALHEAP_SIZE_CLASS_HH_')

    return lines

def output_to_file(file_name, lines):
    f = open(file_name, 'w')
    for l in lines:
        f.write(l)



output_to_file('size_class.cc', make_size_class_cc([ [8,512,8], [512,1024,64], [1024,8192,512], [8192,16384,1024], [16384, 32768, 2048], [16384, 262144, 16384] ]))
output_to_file('size_class.hh', make_size_class_hh([ [8,512,8], [512,1024,64], [1024,8192,512], [8192,16384,1024], [16384, 32768, 2048], [16384, 262144, 16384] ]))
