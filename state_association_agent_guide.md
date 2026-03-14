# State Senior Care Association Directory: Agent Reference

**version:** 3.0
**date:** 2026-03-13
**purpose:** Machine-readable reference for an autonomous agent performing facility-level contact research (Administrator, Executive Director, Director of Nursing) across US senior care facilities.

## Agent Instructions

1. For a given facility, identify its state and facility type (SNF, ALF, or IL).
2. Find the matching state entry below and select the source(s) where `contacts_available` is non-empty.
3. If `login_required: true`, a pre-authenticated browser session is needed before scraping. Check `access_notes` for session details.
4. If `url_pattern` is provided, construct the facility URL directly using the facility name slug. Otherwise, use the `directory_url` and search or browse to the facility.
5. Fields in `contacts_available` indicate what data can be extracted from that source.

## Field Schema

Each source block contains:
- `type`: `SNF` | `ALF` | `IL` | `SNF/ALF` (combined)
- `association_name`: Full name of the association
- `association_url`: Homepage of the association
- `directory_url`: Direct URL to the facility directory or finder
- `login_required`: `true` if a member login is needed to access contact data
- `access_notes`: Instructions for the agent on how to access or navigate the source
- `contacts_available`: List of contact fields publicly exposed per facility. Possible values: `administrator_name`, `administrator_phone`, `administrator_email`, `executive_director_name`, `executive_director_phone`, `director_of_nursing_name`
- `url_pattern`: URL template for direct facility page lookup, if available. Use `{slug}` as placeholder for the slugified facility name (lowercase, hyphens).
- `verified`: `true` if a real facility page was manually confirmed during research

---

## States With Publicly Accessible Contact Data

The following states have at least one source with `contacts_available` non-empty and `login_required: false`.

```yaml
state: Washington
sources:
  - type: SNF/ALF
    association_name: Washington Health Care Association (WHCA)
    association_url: https://www.whca.org
    directory_url: https://www.whca.org/facility-finder/
    login_required: false
    contacts_available:
      - executive_director_name
      - executive_director_phone
    url_pattern: https://www.whca.org/facility-finder/{slug}/
    access_notes: Individual facility pages render via JavaScript. Use Firecrawl or a headless browser. The slug is typically the full facility name lowercased with spaces replaced by hyphens.
    verified: true
```

```yaml
state: Virginia
sources:
  - type: SNF/ALF
    association_name: Virginia Health Care Association - Virginia Center for Assisted Living (VHCA-VCAL)
    association_url: https://www.vhca.org
    directory_url: https://www.vhca.org/locator/
    login_required: false
    contacts_available:
      - administrator_name
      - administrator_phone
    url_pattern: https://www.vhca.org/locator/{slug}/
    access_notes: Individual facility pages list Administrator name and direct phone. Slug pattern matches facility name lowercased with hyphens.
    verified: true
  - type: ALF
    association_name: Virginia Assisted Living Association (VALA)
    association_url: https://www.valainfo.org
    directory_url: https://www.valainfo.org/findassistedlivingcommember.asp
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Search-based directory. Submit facility name via the search form to retrieve the listing.
    verified: true
```

```yaml
state: North Dakota
sources:
  - type: SNF/ALF
    association_name: North Dakota Long Term Care Association (NDLTCA)
    association_url: https://ndltca.org
    directory_url: https://ndltca.org/membership-facilities/
    login_required: false
    contacts_available:
      - administrator_name
      - administrator_phone
      - director_of_nursing_name
    url_pattern: null
    access_notes: Facility listings are on a single paginated page. Scrape the full page and match by facility name. Each entry lists CEO/Administrator, DON, and Social Worker with phone numbers.
    verified: true
```

```yaml
state: Montana
sources:
  - type: SNF
    association_name: Montana Health Care Association (MHCA)
    association_url: https://www.montanahealthcareassociation.org
    directory_url: https://www.montanahealthcareassociation.org/skilled-nursing-facilities.html
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: All SNF facilities are listed inline on a single page. Scrape the page and match by facility name.
    verified: true
  - type: ALF
    association_name: Montana Health Care Association (MHCA)
    association_url: https://www.montanahealthcareassociation.org
    directory_url: https://www.montanahealthcareassociation.org/assisted-living-facilities.html
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: All ALF facilities are listed inline on a single page. Scrape the page and match by facility name.
    verified: true
```

```yaml
state: Indiana
sources:
  - type: SNF
    association_name: Indiana Health Care Association (IHCA) / Indiana state government directory
    association_url: https://www.ihca.org
    directory_url: https://www.in.gov/health/reports/QAMIS/ltcdir/wdirLtc.htm
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: State government directory. Search by facility name. Lists Administrator name per facility.
    verified: true
  - type: ALF
    association_name: Indiana Assisted Living Association (INALA) / Indiana state government directory
    association_url: https://www.inassistedliving.org
    directory_url: https://www.in.gov/health/reports/QAMIS/resdir/wdirRes.htm
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: State government directory for residential care facilities. Lists Administrator name per facility.
    verified: true
```

```yaml
state: Delaware
sources:
  - type: SNF/ALF
    association_name: Delaware Health Care Facilities Association (DHCFA)
    association_url: https://www.dhcfa.org
    directory_url: https://www.dhcfa.org/care-finder/directory/
    login_required: false
    contacts_available:
      - executive_director_name
      - administrator_name
      - director_of_nursing_name
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Directory lists multiple contacts per facility including ED, Admin, and DON.
    verified: true
```

```yaml
state: Pennsylvania
sources:
  - type: SNF
    association_name: Pennsylvania Department of Health (official state directory)
    association_url: https://apps.health.pa.gov
    directory_url: https://apps.health.pa.gov/NCFFacilities/NCFOwnerLicensePull_202404.aspx
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: State government source. Provides a downloadable list of licensed nursing facilities with Administrator names. Not an association directory.
    verified: true
  - type: ALF
    association_name: Pennsylvania Assisted Living Association (PALA)
    association_url: https://pala.org
    directory_url: https://www.pala.org/AF_MemberDirectory.asp?version=1
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Member directory lists a contact person per ALF.
    verified: true
```

```yaml
state: North Carolina
sources:
  - type: SNF
    association_name: North Carolina Health Care Facilities Association (NCHCFA)
    association_url: https://www.nchcfa.org
    directory_url: https://www.nchcfa.org/find-skilled-nursing-care/
    login_required: false
    contacts_available:
      - administrator_name
      - executive_director_name
    url_pattern: null
    access_notes: Directory lists facility-level contacts including Administrator and Executive Director.
    verified: true
  - type: ALF
    association_name: North Carolina Assisted Living Association (NCALA)
    association_url: https://ncala.org
    directory_url: https://ncala.org/ncala-members/
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Member directory lists a contact person per ALF.
    verified: true
```

```yaml
state: Oklahoma
sources:
  - type: SNF
    association_name: Care Providers Oklahoma
    association_url: https://www.careoklahoma.com
    directory_url: https://members.careoklahoma.com/directory/Search/nursing-home-165094
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Directory lists Administrator per SNF facility.
    verified: true
  - type: ALF
    association_name: Care Providers Oklahoma
    association_url: https://www.careoklahoma.com
    directory_url: https://members.careoklahoma.com/directory/Search/assisted-living-165095
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Directory lists Administrator per ALF facility.
    verified: true
```

```yaml
state: Colorado
sources:
  - type: SNF
    association_name: Colorado Health Care Association (COHCA)
    association_url: https://www.cohca.org
    directory_url: https://www.cohca.org/consumers/find-care/
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Directory lists Administrator per SNF facility.
    verified: true
  - type: ALF
    association_name: Colorado Assisted Living Association (CALA)
    association_url: https://www.coloradoassistedlivingassociation.org
    directory_url: https://www.coloradoassistedlivingassociation.org/provider-directory
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Directory lists Administrator per ALF facility.
    verified: true
```

```yaml
state: Idaho
sources:
  - type: SNF
    association_name: Idaho Health Care Association (IHCA)
    association_url: https://www.idhca.org
    directory_url: https://members.idhca.org/atlas/directory/category/skilled-nursing-facility
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Member directory lists a contact person per SNF facility.
    verified: true
  - type: ALF
    association_name: Idaho Health Care Association (IHCA)
    association_url: https://www.idhca.org
    directory_url: https://members.idhca.org/atlas/directory/category/assisted-living-facility
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Member directory lists a contact person per ALF facility.
    verified: true
```

```yaml
state: Maine
sources:
  - type: SNF/ALF
    association_name: Maine Health Care Association (MHCA)
    association_url: https://www.mehca.org
    directory_url: https://www.mehca.org/AF_MemberDirectory.asp
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Directory lists a contact person per facility.
    verified: true
```

```yaml
state: New Mexico
sources:
  - type: SNF/ALF
    association_name: New Mexico Health Care Association / New Mexico Center for Assisted Living (NMHCA/NMCAL)
    association_url: https://www.nmhca.org
    directory_url: https://members.nmhca.org/memberdirectory
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Member directory lists Administrator per facility.
    verified: true
```

```yaml
state: New York
sources:
  - type: SNF/ALF
    association_name: New York State Health Facilities Association / New York State Center for Assisted Living (NYSHFA/NYSCAL)
    association_url: https://nyshfa-nyscal.org
    directory_url: https://www.nyshfa-nyscal.org/find-care-in-ny/
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Combined directory for SNF and ALF. Lists Administrator per facility.
    verified: true
```

```yaml
state: Kansas
sources:
  - type: SNF
    association_name: Kansas Department for Aging and Disability Services (KDADS) - state database
    association_url: https://www.kdads.ks.gov
    directory_url: https://www.kdads.ks.gov/licensing-and-certification/nursing-facilities
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: State government source. KDADS database provides administrator names for SNFs. The KHCA/KCAL association finder does not list named contacts.
    verified: false
  - type: ALF
    association_name: Kansas Health Care Association / Kansas Center for Assisted Living (KHCA/KCAL)
    association_url: https://www.khca.org
    directory_url: https://www.khca.org/consumer-resources/find-care-in-kansas/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Combined finder for SNF and ALF. Lists facility info only.
    verified: true
```

```yaml
state: Iowa
sources:
  - type: SNF
    association_name: Iowa Health Care Association (IHCA)
    association_url: https://iowahealthcare.org
    directory_url: https://iowahealthcare.org/directory/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Directory lists facility info only. No named contacts.
    verified: true
  - type: ALF
    association_name: Iowa Assisted Living Association (IALA)
    association_url: https://ialaonline.net
    directory_url: https://www.ialaonline.net/certified-members
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Member directory lists a contact person per ALF facility.
    verified: true
```

```yaml
state: New Jersey
sources:
  - type: SNF
    association_name: Health Care Association of New Jersey (HCANJ)
    association_url: https://hcanj.org
    directory_url: https://www.hcanj.org/facility-finder/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Directory lists facility info only. No named contacts.
    verified: true
  - type: ALF/IL
    association_name: LeadingAge New Jersey & Delaware
    association_url: https://leadingagenjde.org
    directory_url: https://leadingagenjde.org/consumer-resources/find-a-provider/
    login_required: false
    contacts_available:
      - executive_director_name
      - administrator_name
      - director_of_nursing_name
    url_pattern: null
    access_notes: Directory lists contacts for ALFs and CCRCs/ILs.
    verified: true
```

```yaml
state: Rhode Island
sources:
  - type: SNF
    association_name: Rhode Island Health Care Association (RIHCA)
    association_url: https://www.rihca.com
    directory_url: https://www.rihca.com/membership/facility-members/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Directory lists facility info only. No named contacts.
    verified: true
  - type: ALF
    association_name: Rhode Island Assisted Living Association (RIALA)
    association_url: https://www.riala.org
    directory_url: https://www.riala.org/find-assisted-living#/
    login_required: false
    contacts_available:
      - executive_director_name
    url_pattern: null
    access_notes: Directory lists Executive Director per ALF facility.
    verified: true
```

```yaml
state: South Carolina
sources:
  - type: SNF
    association_name: South Carolina Health Care Association (SCHCA)
    association_url: https://www.schca.org
    directory_url: https://www.schca.org/facility-finder/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Directory lists facility info only. No named contacts.
    verified: true
  - type: ALF
    association_name: South Carolina Association of Community Residential Programs (SCACRP)
    association_url: https://scarcp.net
    directory_url: https://scarcp.net/member-directory/
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Member directory lists a contact person per ALF facility.
    verified: true
```

```yaml
state: Tennessee
sources:
  - type: SNF
    association_name: Tennessee Health Care Association (THCA)
    association_url: https://thca.org
    directory_url: https://www.thca.org/facility-finder/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: SNF section of finder lists facility info only.
    verified: true
  - type: ALF
    association_name: Tennessee Health Care Association / Tennessee Center for Assisted Living (TNCAL)
    association_url: https://thca.org
    directory_url: https://www.thca.org/facility-finder/
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: ALF section of the combined finder reportedly lists a contact person. Verify before use.
    verified: false
```

```yaml
state: Alabama
sources:
  - type: SNF
    association_name: Alabama Nursing Home Association (ANHA)
    association_url: https://anha.org
    directory_url: https://anha.org/facility-locator/
    login_required: false
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Facility locator lists Administrator per SNF. Verify individual facility pages.
    verified: false
  - type: ALF
    association_name: Assisted Living Association of Alabama (ALAA)
    association_url: https://alaaweb.org
    directory_url: null
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: No public online directory. Contact the association directly for a member list.
    verified: true
```

```yaml
state: Hawaii
sources:
  - type: SNF
    association_name: Healthcare Association of Hawaii (HAH)
    association_url: https://www.hah.org
    directory_url: https://www.hah.org/membership-list
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only. No named contacts.
    verified: true
  - type: ALF
    association_name: Association of Residential Care Administrators (ARCA)
    association_url: https://carehomeshawaii.com
    directory_url: https://carehomeshawaii.com/wp-content/uploads/2025/05/CAREHOMES.pdf
    login_required: false
    contacts_available:
      - administrator_name
      - administrator_phone
    url_pattern: null
    access_notes: PDF document listing Adult Residential Care Homes with admin name and phone. Download and parse the PDF.
    verified: true
```

---

## States With Login-Required Directories

The following states have directories that list contact data but require a member login to access.

```yaml
state: Florida
sources:
  - type: SNF
    association_name: Florida Health Care Association (FHCA)
    association_url: https://www.fhca.org
    directory_url: https://www.fhcadirectory.org/directory/?ill_mobiledir_main=1
    login_required: true
    contacts_available:
      - administrator_name
      - administrator_email
    url_pattern: https://www.fhcadirectory.org/directory/{slug}/
    access_notes: >
      Login is required via https://www.fhca.org/membership/directory before accessing
      the directory app at fhcadirectory.org. A pre-authenticated browser session
      (stored cookies) can bypass the login gate. Once authenticated, individual
      facility pages list Administrator name and email. Note: Not all operators are
      FHCA members. Verify the target facility appears in the directory before
      attempting to scrape.
    verified: true
  - type: ALF
    association_name: Florida Assisted Living Association (FALA)
    association_url: https://www.fala.org
    directory_url: https://www.fala.org/facility-members
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only. No named contacts.
    verified: true
```

```yaml
state: Illinois
sources:
  - type: SNF
    association_name: Illinois Health Care Association (IHCA)
    association_url: https://www.ihca.com
    directory_url: https://www.ihca.com/membership-directory/
    login_required: true
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Full member directory requires a login. Public-facing version lists facility info only.
    verified: false
  - type: ALF/IL
    association_name: LeadingAge Illinois
    association_url: https://leadingageil.org
    directory_url: https://directory.leadingageil.org/directory/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Public directory lists facility info only. No named contacts.
    verified: true
```

```yaml
state: South Dakota
sources:
  - type: SNF/ALF
    association_name: South Dakota Health Care Association (SDHCA)
    association_url: https://www.sdhca.org
    directory_url: https://www.sdhca.org/membership
    login_required: true
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Membership directory requires a login. The public Dakota@Home state directory lists facility info only.
    verified: false
```

```yaml
state: Utah
sources:
  - type: SNF
    association_name: Utah Health Care Association (UHCA)
    association_url: https://uthca.org
    directory_url: https://uthca.org/membership/find-a-facility
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Public finder lists facility info only. No named contacts.
    verified: true
  - type: ALF
    association_name: Utah Assisted Living Association (UALA)
    association_url: https://utahassistedliving.org
    directory_url: https://utahassistedliving.org/find_a_preferred_community.php
    login_required: true
    contacts_available:
      - administrator_name
    url_pattern: null
    access_notes: Full directory requires a member login. Public version lists facility info only.
    verified: false
```

---

## States With No Named Contacts in Any Public Directory

For these states, no association directory publicly lists named facility contacts. Alternative sources are noted where applicable.

```yaml
state: Alaska
sources:
  - type: SNF
    association_name: Alaska Hospital & Healthcare Association (AHHA)
    association_url: https://www.alaskahha.org
    directory_url: https://www.alaskahha.org/member-facilities
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
  - type: ALF
    association_name: Assisted Living Association of Alaska (ALAA)
    association_url: https://www.alaa-alaska.org
    directory_url: null
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: >
      No public directory. However, the Alaska DHSS publishes an Excel file of
      licensed ALFs that includes Admin First Name, Admin Last Name, and email.
      URL: https://health.alaska.gov/media/lb0a4b2q/assisted-living-open-facilities-list.xlsx
    verified: true
```

```yaml
state: Arizona
sources:
  - type: SNF
    association_name: Arizona Health Care Association (AZHCA)
    association_url: https://www.azhca.org
    directory_url: https://www.azhca.org/facility-finder/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
  - type: ALF
    association_name: Arizona Assisted Living Federation of America (AZ ALFA)
    association_url: https://azalfa.org
    directory_url: https://connect.gomembers.com/directories/index.php?id=7garuWksF6Xk
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
```

```yaml
state: Arkansas
sources:
  - type: SNF/ALF
    association_name: Arkansas Health Care Association (AHCA)
    association_url: https://arhealthcare.com
    directory_url: https://arhealthcare.com/find-a-facility
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Lists facility info only.
    verified: true
```

```yaml
state: California
sources:
  - type: SNF
    association_name: California Association of Health Facilities (CAHF)
    association_url: https://www.cahf.org
    directory_url: https://www.cahfbuyersguide.com/member-directory
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
  - type: ALF
    association_name: California Assisted Living Association (CALA)
    association_url: https://caassistedliving.org
    directory_url: https://www.caassistedliving.org/CALA/Residents___Families/Find_a_CALA_Community.aspx
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Directory was blocked by CAPTCHA during research. Likely lists facility info only.
    verified: false
```

```yaml
state: Connecticut
sources:
  - type: SNF
    association_name: Connecticut Association of Health Care Facilities (CAHCF)
    association_url: https://www.cahcf.org
    directory_url: https://www.cahcf.org/find-a-facility/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
  - type: ALF
    association_name: Connecticut Assisted Living Association (CALA)
    association_url: https://ctassistedliving.com
    directory_url: https://ctassistedliving.com/our-providers/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
```

```yaml
state: Georgia
sources:
  - type: SNF
    association_name: Georgia Health Care Association (GHCA)
    association_url: https://www.ghca.info
    directory_url: https://directory.ghca.info/
    login_required: false
    contacts_available: []
    url_pattern: https://directory.ghca.info/listing/{slug}/
    access_notes: Individual facility pages exist but do not consistently display named contacts. Verify before use.
    verified: true
  - type: ALF
    association_name: Georgia Senior Living Association (GSLA)
    association_url: https://www.gasla.org
    directory_url: https://web.gasla.org/prod/search
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
```

```yaml
state: Kentucky
sources:
  - type: SNF
    association_name: Kentucky Association of Health Care Facilities (KAHCF)
    association_url: https://kahcfkcal.org
    directory_url: https://www.medicare.gov/care-compare/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: No association directory with named contacts. Medicare.gov Care Compare is the best public fallback for SNFs.
    verified: true
  - type: ALF
    association_name: Kentucky Assisted Living Facilities Association (KALFA)
    association_url: http://kentuckyassistedliving.org
    directory_url: https://kentuckyseniorliving.org/communities
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
```

```yaml
state: Louisiana
sources:
  - type: SNF
    association_name: Louisiana Nursing Home Association (LNHA)
    association_url: https://lnha.org
    directory_url: https://www.lnha.org/findafacility/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
  - type: ALF
    association_name: Mississippi-Louisiana Assisted Living Association (MSLALA)
    association_url: https://mslala.org
    directory_url: null
    login_required: true
    contacts_available: []
    url_pattern: null
    access_notes: Member directory is not publicly accessible.
    verified: false
```

```yaml
state: Maryland
sources:
  - type: SNF/ALF
    association_name: LifeSpan Network
    association_url: https://www.lifespan-network.org
    directory_url: https://www.lifespan-network.org/find-care/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Lists facility info only.
    verified: true
```

```yaml
state: Massachusetts
sources:
  - type: SNF/ALF
    association_name: Massachusetts Senior Care Association
    association_url: https://maseniorcare.org
    directory_url: https://maseniorcare.org/find-a-facility
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
```

```yaml
state: Michigan
sources:
  - type: SNF/ALF
    association_name: Health Care Association of Michigan (HCAM)
    association_url: https://hcam.org
    directory_url: https://www.hcam.org/find-care/facility-finder/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Lists facility info only.
    verified: true
```

```yaml
state: Minnesota
sources:
  - type: SNF/ALF
    association_name: Care Providers of Minnesota / LeadingAge Minnesota
    association_url: https://www.careproviders.org
    directory_url: https://www.health.state.mn.us/facilities/regulation/directory/providerselect.html
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: State DOH directory is the best public source. Lists facility info only.
    verified: true
```

```yaml
state: Mississippi
sources:
  - type: SNF/ALF
    association_name: Mississippi Health Care Association (MSHCA)
    association_url: https://www.mshca.com
    directory_url: https://www.mshca.com/directory/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Lists facility info only.
    verified: true
```

```yaml
state: Missouri
sources:
  - type: SNF/ALF
    association_name: Missouri Health Care Association (MHCA)
    association_url: https://www.mohealthcare.com
    directory_url: https://www.mohealthcare.com/facility-directory/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Directory reportedly lists Administrator but could not be fully verified. Attempt manual lookup.
    verified: false
```

```yaml
state: Nebraska
sources:
  - type: SNF/ALF
    association_name: Nebraska Health Care Association (NHCA)
    association_url: https://nehca.org
    directory_url: https://nehca.org/NEHCA/Directory/Facility-Finder.aspx
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Lists facility info only.
    verified: true
```

```yaml
state: Nevada
sources:
  - type: SNF/ALF
    association_name: Nevada Health Care Association (NVHCA)
    association_url: https://nvhca.org
    directory_url: https://nvhca.org/find-care/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Lists facility info only.
    verified: true
```

```yaml
state: New Hampshire
sources:
  - type: SNF
    association_name: New Hampshire Health Care Association (NHHCA)
    association_url: https://www.nhhca.org
    directory_url: https://www.nhhca.org/find-care/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
  - type: ALF
    association_name: New Hampshire Association of Residential Care Homes (NHARCH)
    association_url: https://nharch.org
    directory_url: https://nharch.org/member-directory/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
```

```yaml
state: Ohio
sources:
  - type: SNF
    association_name: Ohio Health Care Association (OHCA)
    association_url: https://ohca.org
    directory_url: https://www.ohca.org/pages/facility_finder.aspx
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
  - type: ALF
    association_name: Ohio Assisted Living Association (OALA)
    association_url: https://ohioassistedliving.org
    directory_url: https://ohioassistedliving.org/aws/OALA/directory/user_run?group=15463&size=0&submit=Search
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Directory was inaccessible/broken during research. Verify before use.
    verified: false
```

```yaml
state: Oregon
sources:
  - type: SNF/ALF
    association_name: Oregon Health Care Association (OHCA)
    association_url: https://www.ohca.com
    directory_url: https://www.ohca.com/facility-finder/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Lists facility info only.
    verified: true
```

```yaml
state: Texas
sources:
  - type: SNF
    association_name: Texas Health Care Association (THCA)
    association_url: https://txhca.org
    directory_url: https://txhca.org/choose-the-right-facility/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only. TX HHS publishes Excel files of licensed facilities but they were corrupted during research.
    verified: true
  - type: ALF
    association_name: Texas Assisted Living Association (TALA)
    association_url: https://tala.org
    directory_url: https://tala.org/member-list/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
```

```yaml
state: Vermont
sources:
  - type: SNF/ALF
    association_name: Vermont Health Care Association (VHCA)
    association_url: https://www.vhca.net
    directory_url: https://www.vhca.net/directory/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Lists facility info only.
    verified: true
```

```yaml
state: West Virginia
sources:
  - type: SNF
    association_name: West Virginia Health Care Association (WVHCA)
    association_url: https://www.wvhca.org
    directory_url: https://www.wvhca.org/network/facility-finder
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
  - type: ALF
    association_name: West Virginia Health Care Association (WVHCA)
    association_url: https://www.wvhca.org
    directory_url: https://www.wvhca.org/network/assisted-living
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
```

```yaml
state: Wisconsin
sources:
  - type: SNF/ALF
    association_name: Wisconsin Health Care Association / Wisconsin Center for Assisted Living (WHCA/WiCAL)
    association_url: https://www.whcawical.org
    directory_url: https://www.whcawical.org/facility-finder/
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Directory uses JavaScript rendering. Lists facility info only.
    verified: true
  - type: ALF
    association_name: Wisconsin Assisted Living Association (WALA)
    association_url: https://www.ewala.org
    directory_url: https://www.ewala.org/provider-search
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Lists facility info only.
    verified: true
```

```yaml
state: Wyoming
sources:
  - type: SNF/ALF
    association_name: Wyoming Long Term Care Association (WYLTCA)
    association_url: https://wyltc.com
    directory_url: https://www.wyltc.com/finding-a-nursing-home
    login_required: false
    contacts_available: []
    url_pattern: null
    access_notes: Combined association for SNF and ALF. Lists facility info only.
    verified: true
---
```

---

## Universal Fallback Sources

When no state association directory lists named contacts, use these fallback sources in priority order:

```yaml
fallback_sources:
  - name: Medicare Care Compare
    url: https://www.medicare.gov/care-compare/
    facility_types:
      - SNF
    contacts_available:
      - administrator_name
    login_required: false
    access_notes: >
      Covers all Medicare/Medicaid-certified nursing facilities nationwide.
      Individual facility pages list the current Administrator name.
      Search by facility name or address. Does not cover ALF or IL.
    verified: true

  - name: State Licensing Databases
    url: varies_by_state
    facility_types:
      - SNF
      - ALF
    contacts_available:
      - administrator_name
    login_required: false
    access_notes: >
      Most state health departments publish downloadable Excel or CSV files of
      licensed facilities that include the Administrator name. Search for
      "[STATE] department of health licensed nursing facilities list" or
      "[STATE] assisted living facilities directory Excel". Examples:
      PA DOH: https://apps.health.pa.gov/NCFFacilities/NCFOwnerLicensePull_202404.aspx
      AK DHSS: https://health.alaska.gov/media/lb0a4b2q/assisted-living-open-facilities-list.xlsx
    verified: true

  - name: Operator Corporate Website
    url: varies_by_operator
    facility_types:
      - SNF
      - ALF
      - IL
    contacts_available:
      - executive_director_name
    login_required: false
    access_notes: >
      Many large operators (e.g., Brookdale at brookdale.com, Sunrise at sunriseseniorliving.com)
      list the Executive Director on individual community pages. Navigate to the
      operator's website, find the community page, and check for a staff or
      leadership section.
    verified: true
```
