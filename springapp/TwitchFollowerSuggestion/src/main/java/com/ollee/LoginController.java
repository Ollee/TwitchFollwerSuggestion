package com.ollee;

import java.util.Iterator;
import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.ModelAndView;
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
		// TODO check if valid username, if not return to /index, else run gather info code
		
		//gather info and return to usernameResult
		ModelAndView mv = new ModelAndView("usernameResult");
		TwitchWrapper twitch = new TwitchWrapper();
		List<Follow> userFollows = twitch.getUserChannelsFollowed(username.getName());
		System.out.println(username.getName() + " follows a number of users = " + userFollows.size());
		
		CassandraDriver cassandraDriver = new CassandraDriver();
		
		Iterator<Follow> iteratorForFollows = userFollows.iterator();
		while (iteratorForFollows.hasNext()){
			Follow f = iteratorForFollows.next();
			cassandraDriver.insertFollow(f.getUser().getName().toString().toLowerCase(), f.getChannel().getName().toString().toLowerCase());
		}
		
//		int lazyTest = cassandraDriver.insertFollowList(userFollows);
//		
//		if(lazyTest == 1){
//			mv.addObject("message", "failure.");
//		}
//		else{
//			mv.addObject("message", "success.");
//		}

		mv.addObject("username", username);
		mv.addObject("userfollows", userFollows);
		System.out.println("hit LoginController.greetingSubmit for user: " + username.getName());
		return mv;
	}
}
