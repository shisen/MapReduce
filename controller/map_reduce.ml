open Util
open Worker_manager

(* TODO implement these *)
let map kv_pairs shared_data map_filename : (string * string) list = 
  let manager= initialize_mappers map_filename shared_data in
  let the_pool= Thread_pool.create 200 in
  let output = ref [] in
  let write_lock = Mutex.create() in
  let input = Hashtbl.create 50 in 
  List.fold_left (fun () (k,v)-> Hashtbl.add input k v) () kv_pairs;
  let tbllock= Mutex.create() in 
  let newwork kv_pair ()= 
    let the_mapper= pop_worker manager in  
    let output_one= ref [] in
    let result_one= Worker_manager.map the_mapper (fst kv_pair) (snd kv_pair)
    in
    push_worker manager the_mapper;
    match result_one with
    |None -> ()
    |Some l ->
      output_one:=l;
      Mutex.lock tbllock;
      let found= Hashtbl.mem input (fst kv_pair) in
      Mutex.unlock tbllock;
      if not(found) then ()
      else 
        (Mutex.lock tbllock;
         Hashtbl.remove input (fst kv_pair);
         Mutex.unlock tbllock;
         Mutex.lock write_lock;
         output:= ((!output)@(!output_one));
         Mutex.unlock write_lock)
  in
  print_endline "we start looping";
  while Hashtbl.length input >0
 (*   Mutex.lock tbllock;
    let len= Hashtbl.length input in
    Mutex.unlock tbllock;
    len>0 *)
  do
    print_int (Hashtbl.length input);
    Hashtbl.iter
      (fun k v -> 
        Thread_pool.add_work (newwork (k,v)) the_pool) input;
    Thread.delay 0.1
  done;
  print_endline "we are done looping";
  Thread_pool.destroy the_pool;
  clean_up_workers manager;
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
  let combined = Hashtbl.create 50 in 
  List.fold_left (fun () (k,v)-> Hashtbl.add combined k v) () kvs_pairs;
  let tbllock= Mutex.create() in
  let newwork (pair:string*string list) ()=
    let the_reducer = pop_worker manager in 
    let output_one=ref [] in 
    let result_one=Worker_manager.reduce the_reducer (fst pair) (snd pair) in
    push_worker manager the_reducer;
    match result_one with
    |None -> ()
    |Some l ->
      output_one:= l;
      Mutex.lock tbllock;
      let found= Hashtbl.mem combined (fst pair) in
      Mutex.unlock tbllock;
      if not(found) then ()
      else 
        (Mutex.lock tbllock;
         Hashtbl.remove combined (fst pair);
         Mutex.unlock tbllock;
         Mutex.lock write_lock;
         output:= (fst pair, !output_one)::(!output);
         Mutex.unlock write_lock)
  in 
  while Hashtbl.length combined >0 do
    Hashtbl.iter 
      (fun str str_lst-> 
        Thread_pool.add_work (newwork (str,str_lst)) the_pool) combined;
    Thread.delay 0.1
  done;
  Thread_pool.destroy the_pool;
  clean_up_workers manager;
  !output

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
(*  let ()= print_map_results mapped in *)
  let combined = combine mapped in
  (* The next line is added for debugging purpose*)
(*  let ()= print_combine_results combined in *)
  let reduced = reduce combined  "" (app_dir ^ reducer ^ ".ml") in
  (titles, reduced)
