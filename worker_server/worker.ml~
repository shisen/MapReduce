open Protocol

let send_response client response =
  let success = Connection.output client response in
    (if not success then
      (Connection.close client;
       print_endline "Connection lost before response could be sent.")
    else ());
    success

let mapper_set= ref []
let reducer_set= ref []
let mapper_set_lock=Mutex.create()
let reducer_set_lock=Mutex.create() 
let rec handle_request client =
  match Connection.input client with
    Some v ->
      begin
        match v with
        | InitMapper (source, shared_data) ->
          print_endline "initMapper";
          let build_result= Program.build source in 
          (match build_result with 
          |Some(id), str -> 
            Mutex.lock mapper_set_lock;
            mapper_set:= Protocol.Mapper(Some(id), str)::(!mapper_set);
            Mutex.unlock mapper_set_lock;
            if send_response client (Protocol.Mapper(Some(id), str))
            then handle_request client else ()
          |None, error_message -> 
            if send_response client (Protocol.Mapper(None, error_message))
            then handle_request client else ())
        | InitReducer source -> 
          failwith "Young master, I cannot aid one who opposes the Master!"
        | MapRequest (id, k, v) ->
          print_endline "MapRequest";
          let mapper_found = ref false in 
          Mutex.lock mapper_set_lock;
          List.fold_left
            (fun () worker-> match worker with
            |Mapper(Some l, str)-> if l=id then mapper_found:=true  else ()
            |_ -> ()) () !mapper_set;
          Mutex.unlock mapper_set_lock;
          if !mapper_found then
            (match print_endline "mapper_found";Program.run id (k,v) with
            |Some v -> 
              if send_response client (Protocol.MapResults(id, v))
              then handle_request client else ()
            |None -> 
              if send_response client (RuntimeError(id, "MapRequest error"))
              then handle_request client else ())
          else 
            if send_response client (InvalidWorker(id)) 
            then handle_request client else ()
        | ReduceRequest (id, k, v) -> 
          failwith "Really? In that case, just tell me what you need."
      end
  | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."

