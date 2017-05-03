package com.ollee;

import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import me.philippheuer.twitch4j.model.Channel;

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
		
		//gather info and return to usernameResult
		ModelAndView mv = null;
		
		if(TwitchWrapper.channelExists(username.getName())){
			mv = new ModelAndView("usernameResult");
			Long initial = Instant.now().getEpochSecond();
			//CassandraDriver.threadedInsertFollowList(new LinkedList<Follow>(userFollows));
			TwitchAPICallHandler callHandler = new TwitchAPICallHandler();
			LinkedHashMap<String, Integer> channelSuggestions = callHandler.fetchChannelSuggestions(username.getName());
			Long post = Instant.now().getEpochSecond();
			
			System.out.println("This took this long in Seconds: " + (post - initial));


			LinkedHashMap<Channel,Integer> finalSuggestions = new LinkedHashMap<Channel, Integer>();
			
			Iterator<String> iter = channelSuggestions.keySet().iterator();
			int i = 0;
			String key;
			while(iter.hasNext() && i < 20){
				key = iter.next();
				System.out.println("Final Run #: " + i + " and key: " + key + " and weight : " + channelSuggestions.get(key));
				i++;
				if(TwitchWrapper.channelExists(key)){
					finalSuggestions.put(TwitchWrapper.getChannelObject(key), channelSuggestions.get(key));
				}
			}

			mv.addObject("username", username);
			mv.addObject("suggestions", finalSuggestions);
			System.out.println("LoginController: hit LoginController.greetingSubmit for user: " + username.getName());
		} else {
			mv = new ModelAndView("redirect:/");
		}
		
		
		return mv;
	}
	
	
}
