package com.optimacom;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Info {
    public static void main(String[] args){
        String argument = "<no argument>";
        if(args.length > 0){
            argument = args[0];
        }
        log.info("ok: {}", argument);
    }
}
