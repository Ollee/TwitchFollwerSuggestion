package com.ollee;

import java.util.LinkedList;
import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import me.philippheuer.twitch4j.model.Channel;
import me.philippheuer.twitch4j.model.Follow;

@Controller
@RequestMapping("/")
public class LoginController {
	
	@GetMapping("/")
	public String indexGet(Model model){
		model.addAttribute("username", new UserName());
		System.out.println("hit LoginController.indexGet");
		return "index";
	}
	
	@PostMapping("/")
	public ModelAndView greetingSubmit(@ModelAttribute UserName username){
		// TODO check if valid username, if not return to /index, else run gather info code
		
		//gather info and return to usernameResult
		ModelAndView mv = new ModelAndView("usernameResult");
		//TwitchWrapper twitch = new TwitchWrapper();//replace this with threading controller
		//List<Follow> userFollows = TwitchWrapper.getUserChannelsFollowed(username.getName());
		//System.out.println("LoginController: " + username.getName() + " follows a number of users = " + userFollows.size());
		
		//CassandraDriver.threadedInsertFollowList(new LinkedList<Follow>(userFollows));
		List<Channel> userFollows = TwitchAPICallHandler.fetchChannelSuggestions(username.getName());
		
		
		mv.addObject("username", username);
		mv.addObject("userfollows", userFollows);
		System.out.println("LoginController: hit LoginController.greetingSubmit for user: " + username.getName());
		return mv;
	}
}
