package com.ollee;

import java.sql.Date;
import java.time.Instant;
import java.util.Iterator;
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
@RequestMapping(value={"/","/error"})
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
		ModelAndView mv = null;
		
		if(TwitchWrapper.channelExists(username.getName())){
			mv = new ModelAndView("usernameResult");
			Long initial = Instant.now().getEpochSecond();
			//CassandraDriver.threadedInsertFollowList(new LinkedList<Follow>(userFollows));
			TwitchAPICallHandler callHandler = new TwitchAPICallHandler();
			List<String> userFollows = callHandler.fetchChannelSuggestions(username.getName());
			Long post = Instant.now().getEpochSecond();
			
			System.out.println("This took this long in Seconds: " + (post - initial));
			
			List<Channel> suggestions = new LinkedList<Channel>();
			
			
			
			Iterator<String> iter = userFollows.iterator();
			int i = 0;
			while(iter.hasNext() && i < 20){
				i++;
				suggestions.add(TwitchWrapper.getChannelObject(iter.next()));
				System.out.println("Final Run #: " + i);
			}
			
			for (int j = 0; j < suggestions.size(); j++){
				System.out.println("dumping channel Suggestions: " + suggestions.get(j).getName().toLowerCase());
			}
			
			
			mv.addObject("username", username);
			mv.addObject("suggestions", suggestions);
			System.out.println("LoginController: hit LoginController.greetingSubmit for user: " + username.getName());
		} else {
			mv = new ModelAndView("redirect:/");
		}
		
		
		return mv;
	}
	
	
}
