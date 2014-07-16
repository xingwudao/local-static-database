#ifndef VIRTUAL_MEMORY_MAPPER_H
#define VIRTUAL_MEMORY_MAPPER_H

#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>  
#include <stdlib.h>
#include <fcntl.h> 
#include <stdint.h> 
#include <string>
#include <iostream>

namespace kaijiang_api
{
	class VirtualMemoryMapper
	{
		private:
			void* virtual_memory;
			int32_t file_handle;
			std::string file_name;
			size_t size;
		private:
			// description: clean resource
			// parameters:
			//  nothing
			// return:
			//  nothing
			void Clean()
			{
				if(file_handle >= 0)
					close(file_handle);
				file_name = "";
				munmap(virtual_memory, size);
				virtual_memory = NULL;
				size = 0;
			}
		public:
			// description: constructor
			// parameters:
			//  [IN] binary_data_file -- the file
			// return:
			//  nothing
			VirtualMemoryMapper(const char* data_file)
			{
				size = 0;
				virtual_memory = NULL;
				file_handle = open(data_file, O_RDONLY);
				if(file_handle < 0)
				{
                    std::cerr << data_file << " can not be opened." << std::endl;
					return;
				}
				struct stat st; 
				if(fstat(file_handle, &st) == -1)
				{
                    std::cerr << data_file << " is null." << std::endl;
					close(file_handle);
					return;
				}
				virtual_memory = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, file_handle, 0);
				if(NULL == virtual_memory || (void*)-1 == virtual_memory)
				{
					virtual_memory = NULL;
                    std::cerr << data_file << " map to virtual memory failed." << std::endl;
					close(file_handle);
					return;
				}
				size = st.st_size;
				file_name = std::string(data_file);
			}

			VirtualMemoryMapper(const VirtualMemoryMapper& another)
			{
				VirtualMemoryMapper(another.file_name.c_str());
			}

			VirtualMemoryMapper& operator=(const VirtualMemoryMapper& another)
			{
				if(this == &another)
					return *this;
				
				Clean();

				VirtualMemoryMapper(another.file_name.c_str());

				return *this;
			}

			virtual ~VirtualMemoryMapper()
			{
				Clean();
			}

			// descrption: get data from virtual memory
			// parameters:
			//  [IN] off -- the offset read from
			//  [IN] size -- the size to read
			// return:
			//  data, NULL if copy failed.
			//char* Copy(uint64_t off, uint64_t size)
			//{
			//	if(off + size > this->size)
			//		return NULL;

			//	char* cpy = new char[size];
			//	if(cpy == NULL)
			//		return NULL;

			//	strncpy(cpy, (char*)virtual_memory + off, size);

			//	return cpy;
			//}
			const void* GetData() const
			{
				return  virtual_memory;
			}

			// description: get file name
			const inline std::string GetFileName() const {return file_name;}

			// description: get size
			inline uint32_t GetSize() const {return size; }
	};
};

#endif
