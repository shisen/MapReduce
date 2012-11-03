type ('a, 'b) t =
    { items: int ref;
      table: ((('a * 'b) list) array) ref;
      hash: 'a -> int
    }

let create capacity hash = 
  { items = ref 0; 
    table = ref (Array.make capacity []); 
    hash = hash }

let rec add table key value =   
  if !(table.items) >= Array.length !(table.table) * 3 / 4 then 
    resize table;
  let i = table.hash key mod Array.length !(table.table) in 
  !(table.table).(i) <- (key,value)::!(table.table).(i);
  table.items := !(table.items) + 1

and resize h =
  let n = Array.length !(h.table) in
  let h' = create (2 * n) h.hash in
  for i = 0 to n - 1 do
    List.iter
      (fun (k,v) -> add h' k v) 
      !(h.table).(i)
  done;
  h.table := !(h'.table)

let find table key =
  let i = table.hash key mod Array.length !(table.table) in
  List.assoc key !(table.table).(i) 

let mem table key = 
  try find table key; true
  with Not_found -> false

let remove table key =
  let i = table.hash key mod Array.length !(table.table) in 
  let rec pop acc l = 
    match l with 
    | [] -> (false,List.rev acc)
    | (k',v)::t -> if k' = key then (true,List.rev acc @ t) else pop ((k',v)::acc) t in 
  let b,l' = pop [] !(table.table).(i) in 
  !(table.table).(i) <- l';
  if b then table.items := !(table.items) - 1 else ()

let iter f table = 
  let n = Array.length !(table.table) in
  for i = 0 to n - 1 do
    List.iter
      (fun (k,v) -> f k v) 
      !(table.table).(i)
  done

let fold f table init = failwith ""
  (*Array.fold_left (List.fold_left (fun (k,v) ->  ) table init *)
(* ('a -> 'b -> 'c -> 'c) -> ('a, 'b) t -> 'c -> 'c *)
(* ('a -> 'b -> 'a) -> 'a -> 'b list -> 'a *)
(* let n = Array.length !(table.table) in *)
(* let init := ref init; *)
(*   for i = 0 to n - 1 do *)
(*     let init := ref (List.fold_left  *)
(*       (fun (k,v) -> f k v !init)  *)
(*       !init (!(table.table).(i))) *)
(*   done *)

let length table = 
  Array.length !(table.table)
