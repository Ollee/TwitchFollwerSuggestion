package com.ollee;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class LoginController {
	
	@GetMapping("/index")
	public String indexGet(Model model){
		model.addAttribute("username", new UserName());
		System.out.println("hit LoginController.indexGet");
		return "index";
	}
	
	@PostMapping("/index")
	public ModelAndView greetingSubmit(@ModelAttribute UserName username){
		ModelAndView mv = new ModelAndView("usernameResult");
		mv.addObject("username", username);
		System.out.println("hit greetingSubmit method for user: " + username.getName());
		return mv;
	}
}
