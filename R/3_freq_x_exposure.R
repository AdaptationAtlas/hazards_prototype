retry_risk_x_exposure <- function(f, 
                                  save_dir,
                                  variable,
                                  overwrite,
                                  crop_exposure_path,
                                  livestock_exposure_path,
                                  round_n,
                                  crop_choices,
                                  max_tries = 3,
                                  sleep_sec = 2,
                                  debug_mode = FALSE) {
  
  if (debug_mode) {
    # Run directly with error propagation
    risk_x_exposure(file = f,
                    save_dir = save_dir,
                    variable = variable,
                    overwrite = overwrite,
                    crop_exposure_path = crop_exposure_path,
                    livestock_exposure_path = livestock_exposure_path,
                    crop_choices = crop_choices,
                    round_n = round_n,
                    verbose = TRUE)  # verbose helpful in debug
    return(NULL)
  }
  
  # Retry loop if not in debug mode
  for (k in seq_len(max_tries)) {
    try({
      risk_x_exposure(file = f,
                      save_dir = save_dir,
                      variable = variable,
                      overwrite = overwrite,
                      crop_exposure_path = crop_exposure_path,
                      livestock_exposure_path = livestock_exposure_path,
                      crop_choices = crop_choices,
                      round_n = round_n,
                      verbose = FALSE)
      return(NULL)  # success
    }, silent = TRUE)
    Sys.sleep(sleep_sec)
  }
  return(as.character(f))  # failure after retries
}