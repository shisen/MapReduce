open Util

let main (args : string array) : unit =
  if Array.length args < 3 then
    Printf.printf "Usage: word_count <filename>"
  else begin
    let filename = args.(2) in
    let (_, results) = 
      Map_reduce.map_reduce "word_count" "mapper" "reducer" filename in
    print_reduce_results results end
