let (key, values) = Program.get_input() in
let file_list= 
  List.fold_left (fun acc v -> if List.mem v acc then acc else v::acc) 
    [] values in
Program.set_output [String.concat " " file_list]
