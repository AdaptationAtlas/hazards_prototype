#' Retry Download
#'
#' This function attempts to download a file from a given URL multiple times
#'
#' @param dl_url A character string specifying the URL to download from
#' @param destfile A character string specifying the destination file path
#' @param n An integer specifying the number of times to try downloading the file
#' @return A character string indicating if the file was downloaded successfully or a warning if all attempts failed
#' @examples
#' try_download("https://example.com/file.txt", "C:/Downloads/file.txt", 3)
#' try_download("https://example.com/file.txt", "C:/Downloads/file.txt", 5)
try_download <- function(dl_url, destfile, n) {
  success <- FALSE  # Flag to track successful result
  result <- NULL  # Variable to store the result
  
  for (i in 1:n) {
    tryCatch(
      {
        download.file(dl_url, destfile, quiet = TRUE)
        success <- TRUE  # Set flag to indicate success
        break  # Exit the loop if successful result obtained
      },
      error = function(e) {
        message(paste("Error retrying...", i, "/", n, sep = ""))
      }
    )
  }
  
  if (success) {
    "File downloaded successfully"
  } else {
    warning("Error: All attempts failed.")  # or any other desired error message
  }
}