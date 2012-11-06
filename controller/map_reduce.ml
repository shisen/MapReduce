open Util
open Worker_manager

(* TODO implement these *)
let map kv_pairs shared_data map_filename : (string * string) list = 
  let manager= initialize_mappers map_filename shared_data in
  let the_pool= Thread_pool.create 200 in
  let output = ref [] in
  let write_lock = Mutex.create() in
  let newwork kv_pair ()= 
    let the_mapper= pop_worker manager in  
    let output_one= ref [] in
    let value option = match option with
      |Some l -> l
      |None -> failwith "Error occured in map" in
    output_one:=
      value (Worker_manager.map the_mapper (fst kv_pair) (snd kv_pair));
    Mutex.lock write_lock;
    output:= ((!output)@ (!output_one));
    Mutex.unlock write_lock;
    push_worker manager the_mapper
  in 
  List.fold_left (fun () kv_pair->
    Thread_pool.add_work (newwork kv_pair) the_pool) 
    () kv_pairs;
  Thread.delay 2.0;
  clean_up_workers manager;
  Thread_pool.destroy the_pool;
  !output

let combine kv_pairs : (string * string list) list = 
  let arr= ref [||] in 
  let combine_one () (k,v) = 
    let found= ref false in 
    let index= ref 0 in 
    while (not(!found)) && (!index < Array.length !arr) do
      if (fst (!arr.(!index)))= k then 
        (found:= true; 
         !arr.(!index)<- (fst !arr.(!index), v::(snd !arr.(!index))))
      else index:= !index +1
    done;
    if (not(!found)) then
      arr:= Array.append !arr [|(k,[v])|] 
  in
  List.fold_left combine_one () kv_pairs;
  Array.to_list !arr

let reduce kvs_pairs shared_data reduce_filename 
    : (string * string list) list =
  let manager= initialize_reducers reduce_filename shared_data in
  let the_pool = Thread_pool.create 200 in
  let output= ref [] in
  let write_lock = Mutex.create() in
  failwith "to be implemented"

let map_reduce (app_name : string) (mapper : string) 
    (reducer : string) (filename : string) =
  let app_dir = Printf.sprintf "apps/%s/" app_name in
  let docs = load_documents filename in
  (* let titles = Hashtable.create 16 Hashtbl.hash in *)
  (* The above line is replaced with the below line for testing purpose *)
  let titles = Hashtbl.create 16 in 
  let add_document (d : document) : (string * string) =
    let id_s = string_of_int d.id in
    (*  Hashtable.add titles id_s d.title; (id_s, d.body) in *)
    (* The above line is replaced with the below line for testing purpose*)
    Hashtbl.add titles id_s d.title; (id_s, d.body) in
  let kv_pairs = List.map add_document docs in
  let mapped = map kv_pairs "" (app_dir ^ mapper ^ ".ml") in
  (* The next line is added for debugging purpose*)
  let ()= print_map_results mapped in 
  let combined = combine mapped in
  (* The next line is added for debugging purpose*)
  let ()= print_combine_results combined in
  let reduced = reduce combined  "" (app_dir ^ reducer ^ ".ml") in
  (titles, reduced)
