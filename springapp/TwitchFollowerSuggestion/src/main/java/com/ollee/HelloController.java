package com.ollee;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class HelloController {
	
    @GetMapping("/greeting")
    public String greeting(Model model) {
        model.addAttribute("greeting", new Greeting());
        System.out.println("hit greeting method");
        return "greeting";
    }
    
    @PostMapping("/greeting")
    public String greetingSubmit(@ModelAttribute Greeting greeting){
    	System.out.println("hit greetingSubmit method");
    	return "result";
    }
	
}
