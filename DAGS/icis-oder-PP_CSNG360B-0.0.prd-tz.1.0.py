from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CSNG360B',WORKFLOW_ID='45f84e65de23483abf638292361cc100', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG360B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 25, 13, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('45f84e65de23483abf638292361cc100')

    csng360bJob_vol = []
    csng360bJob_volMnt = []
    csng360bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng360bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng360bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng360bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng360bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng360bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4110d8651e6447da8a3bc14b57ef9155',
        'volumes': csng360bJob_vol,
        'volume_mounts': csng360bJob_volMnt,
        'env_from':csng360bJob_env,
        'task_id':'csng360bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng360bJob"
,"inputDate=20241129"
,"ofcCd=712616"
,"empNo=91361629"
,"empName=CSNG360B"
,"requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('45f84e65de23483abf638292361cc100')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng360bJob,
        Complete
    ]) 

    # authCheck >> csng360bJob >> Complete
    workflow








