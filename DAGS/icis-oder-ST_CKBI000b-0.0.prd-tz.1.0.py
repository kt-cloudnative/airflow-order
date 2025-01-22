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
                , WORKFLOW_NAME='ST_CKBI000b',WORKFLOW_ID='89f4da2221244b7685156ed6aa3f0e78', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ST_CKBI000b-0.0.prd-tz.1.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2024, 12, 31, 14, 19, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('89f4da2221244b7685156ed6aa3f0e78')

    ckbi000bJob_vol = []
    ckbi000bJob_volMnt = []
    ckbi000bJob_vol.append(getVolume('shared-volume','shared-volume'))
    ckbi000bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    ckbi000bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    ckbi000bJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    ckbi000bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    ckbi000bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1e168f2045684c81a5a2983083dc3084',
        'volumes': ckbi000bJob_vol,
        'volume_mounts': ckbi000bJob_volMnt,
        'env_from':ckbi000bJob_env,
        'task_id':'ckbi000bJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.4.1.71',
        'arguments':["--job.name=ckbi000bJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('89f4da2221244b7685156ed6aa3f0e78')

    workflow = COMMON.getICISPipeline([
        authCheck,
        ckbi000bJob,
        Complete
    ]) 

    # authCheck >> ckbi000bJob >> Complete
    workflow








