
library(NCmisc)
library(jsonlite)

clean_data <- function(fn, output_dir) {
    
    start.time <- Sys.time()
    dir.create(output_dir, showWarnings = FALSE)
    sys_split <- paste0("gsplit -d -a 7 -l 5000 ", fn, " ", output_dir, "/")
    system(sys_split)
    setwd(output_dir)
    selected.files <- paste(sprintf("%07d",1:length(list.files(getwd()))-1), "", sep="")
    test_data <- NULL
    clean_file <- paste0("../", fn, "_clean_log")
    
    j <- 0
    
    for (i in selected.files) {
        
        tryCatch({ 
            
            test_data <- jsonlite::stream_in(file(i), handler = function(nix){})
        }, 
            
            error= function(e){cat("ERROR :", i, conditionMessage(e), "\n") 
            
            # create new dir
            dir.create(c <- paste0("corrupt", j), showWarnings = FALSE)
            
            # copy the corrupt file to corrupt directory
            file.copy(from = i, to = c, recursive = FALSE, copy.mode = TRUE)
            
            print(c)
            # set new dir as current dir
            setwd(c)
            # split that file into 500 separate files
            
            system(paste0("gsplit -d -a 5 -l 1 ", i, " 0"))
            file.remove(i)
            
            # get file path and create var that stores all the new files
            cleanme.files <- paste(sprintf("%06d",1:length(list.files(getwd()))-1), "", sep="")
            
                # for loop cycling through 500 files, wrapped in trycatch handler
                for (n in cleanme.files) {
                    
                    tryCatch({ 
                        
                        cleanme_data <- jsonlite::stream_in(file(n), handler = function(nix2){})
                    }, 
                        # if exception thrown, remove that file
                    
                        error = function(e){cat("ERROR :", n, conditionMessage(e), "\n") 
                        file.remove(n)
                        write(n, file = "corruptdata", append = TRUE)
                        cleanme.files <- cleanme.files[-length(cleanme.files)]
         
                     })
                }
                
                corrupt_file <- "corruptdata"
                
                # end of for loop, merge all files into one file
                file.copy(from = corrupt_file, to = paste0("../corruptdata_", j), recursive = FALSE, copy.mode = TRUE)
                file.remove(corrupt_file)
                sys_cat <- paste0("cat * >> all")
                system(sys_cat)   
                setwd("..")
  
            }
        )
    
        j <- j + 1
        write(paste0(i, " clean"), file = clean_file, append = TRUE)
        
    }
    
    setwd("..")
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    print(time.taken)
    
}
