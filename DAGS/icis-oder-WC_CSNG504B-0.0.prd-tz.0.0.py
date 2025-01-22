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
                , WORKFLOW_NAME='WC_CSNG504B',WORKFLOW_ID='76cb1796e50d4446923ac676f20e82c2', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CSNG504B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2023, 11, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('76cb1796e50d4446923ac676f20e82c2')

    csng504bJob_vol = []
    csng504bJob_volMnt = []
    csng504bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng504bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng504bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng504bJob_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csng504bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng504bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '27c365e4a61f4338a2c78ea4d8300158',
        'volumes': csng504bJob_vol,
        'volume_mounts': csng504bJob_volMnt,
        'env_from':csng504bJob_env,
        'task_id':'csng504bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.5',
        'arguments':["--job.name=csng504bJob", "date="+str(datetime.now()), "inputDate=202110", "workMode=0131"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('76cb1796e50d4446923ac676f20e82c2')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng504bJob,
        Complete
    ]) 

    # authCheck >> csng504bJob >> Complete
    workflow








