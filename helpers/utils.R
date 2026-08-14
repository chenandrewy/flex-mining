# Utility helpers: memory inspection, fst IO, string formatting.
#
# Sourced by 0_Environment.R after packages and config.R. These functions
# rely on objects from the sourcing environment (e.g. globalSettings and
# chapter-local data frames); do not source this file in isolation.


## MEMORY CHECK

# improved list of objects
.ls.objects <- function (pos = 1, pattern, order.by,
                         decreasing=FALSE, head=FALSE, n=5) {
  napply <- function(names, fn) sapply(names, function(x)
    fn(get(x, pos = pos)))
  names <- ls(pos = pos, pattern = pattern)
  obj.class <- napply(names, function(x) as.character(class(x))[1])
  obj.mode <- napply(names, mode)
  obj.type <- ifelse(is.na(obj.class), obj.mode, obj.class)
  obj.prettysize <- napply(names, function(x) {
    format(utils::object.size(x), units = "auto") })
  obj.size <- napply(names, object.size)
  obj.dim <- t(napply(names, function(x)
    as.numeric(dim(x))[1:2]))
  vec <- is.na(obj.dim)[, 1] & (obj.type != "function")
  obj.dim[vec, 1] <- napply(names, length)[vec]
  out <- data.frame(obj.type, obj.size, obj.prettysize, obj.dim)
  names(out) <- c("Type", "Size", "PrettySize", "Length/Rows", "Columns")
  if (!missing(order.by))
    out <- out[order(out[[order.by]], decreasing=decreasing), ]
  if (head)
    out <- head(out, n)
  out
}


# shorthand
lsos <- function(..., n=10) {
  .ls.objects(..., order.by="Size", decreasing=TRUE, head=TRUE, n=n)
}



# Convenience function for round numbers in strings
round_numbers_in_strings <- function(strings_with_numbers) {
  regex_pattern <- "\\d+\\.?\\d*" # matches any number with or without decimal point
  rounded_strings <- c() # create an empty vector to store the results
  
  for (string_with_number in strings_with_numbers) {
    # Use regular expressions to extract the number from the string
    number_in_string <- as.numeric(gsub("[^[:digit:].]", "", regmatches(string_with_number, regexpr(regex_pattern, string_with_number))))
    
    # Round the number to two decimal places
    rounded_number <- sprintf("%.1f",number_in_string)  %>% as.character()
    
    # Replace the original number in the string with the rounded number
    string_with_rounded_number <- gsub(regex_pattern, toString(rounded_number), string_with_number)
    
    # Add the result to the output vector
    rounded_strings <- c(rounded_strings, string_with_rounded_number)
  }
  
  return(rounded_strings)
}
