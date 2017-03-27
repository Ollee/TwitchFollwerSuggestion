package com.ollee;

import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.ModelAndView;
import me.philippheuer.twitch4j.*;
import me.philippheuer.twitch4j.model.Follow;

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
		TwitchWrapper twitch = new TwitchWrapper();
		List<Follow> userFollows = twitch.getUserChannelsFollowed(username.getName());
		System.out.println(username.getName() + " follows a number of users = " + userFollows.size());
		
		
		mv.addObject("username", username);
		mv.addObject("userfollows", userFollows);
		System.out.println("hit LoginController.greetingSubmit for user: " + username.getName());
		return mv;
	}
}
