package org.lsmdb;

import org.lsmdb.kvstore.LSMDatabase;

public class Main {
    public static void main(String[] args) throws Exception {
        LSMDatabase db = new LSMDatabase("data/");

        db.put("name", "Likitha");
        db.put("lang", "Java");
        db.flush(); // Forces flush to disk

        System.out.println(db.get("name")); // → Likitha
    }
}
